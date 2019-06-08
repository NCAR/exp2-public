package expPublic;
use warnings;
use strict;
use DBI;
use DBD::mysql;
use Time::localtime;
use DateTime::Format::MySQL;
use Array::Utils qw(:all);
use vars qw(@ISA @EXPORT);
use Exporter;
use CGI::Carp qw(fatalsToBrowser warningsToBrowser); 
use CGI::Session qw/-ip-match/;
use lib "/home/www/html/csegdb/lib";
use config;

@ISA = qw(Exporter);
@EXPORT = qw(getPublicExps);

sub getPublicExps
{
    my $dbh = shift;
    my @exps;
    my ($sql1, $sth1);
    my @process = (8,9,10,11,12,13,21,22,23,24);
    
    # get case data for all cases with an ESGF publish process Verified (6)
    my $sql = qq(select c.id, c.casename, c.compiler, c.compset, c.expType_id,
                 c.grid, c.is_ens, c.machine, c.model_version, c.mpilib, c.title,
                 DATE_FORMAT(j.last_update, '%Y-%m-%d') as last_update, j.disk_usage
                 from t2_cases as c, t2j_status as j 
                 where c.id = j.case_id and
                 j.process_id = 18 and 
                 j.status_id = 6
                 order by c.casename);
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    while (my $ref = $sth->fetchrow_hashref())
    {
	my %exp;

	# load up this exp hash global data
	$exp{'case_id'} = $ref->{'id'};
	$exp{'casename'} = $ref->{'casename'};
	$exp{'compiler'} = $ref->{'compiler'};
	$exp{'compset'} = $ref->{'compset'};
	$exp{'expType_id'} = $ref->{'expType_id'};
	$exp{'grid'} = $ref->{'grid'};
	$exp{'is_ens'} = $ref->{'is_ens'};
	$exp{'machine'} = $ref->{'machine'};
	$exp{'model_version'} = $ref->{'model_version'};
	$exp{'mpilib'} = $ref->{'mpilib'};
	$exp{'title'} = $ref->{'title'};
	$exp{'last_update'} = $ref->{'last_update'};
	$exp{'disk_usage'} = $ref->{'disk_usage'}/(1000 * 1000 * 1000);
	
	$sql1 = qq(select name, description from t2_expType where id = $exp{'expType_id'});
	$sth1 = $dbh->prepare($sql1);
	$sth1->execute();
	($exp{'expType_name'}, $exp{'expType_desc'}) = $sth1->fetchrow();
	$sth1->finish();

	$exp{'ensemble_size'} = 1;
	# get CMIP6 specific fields
	if ($exp{'expType_id'} == 1) {
	    $sql1 = qq(select e.name, e.uid, e.description, j.ensemble_size
                       from t2_cmip6_exps as e, t2j_cmip6 as j
                       where j.case_id = $exp{'case_id'} and
                       e.id = j.exp_id);
	    $sth1 = $dbh->prepare($sql1);
	    $sth1->execute();
	    ($exp{'cmip6_name'}, $exp{'cmip6_uid'}, $exp{'cmip6_desc'}, $exp{'ensemble_size'}) = $sth1->fetchrow();
	    $sth1->finish();
	}
	if ($exp{'ensemble_size'} == 0) {
	    $exp{'ensemble_size'} = 1;
	}

	# TODO - get other experiment type specific information as they become available

	# get ESGF publication links (should be 1)
	$sql1 = qq(select link, description
                   from t2j_links
                   where case_id = $exp{'case_id'} and process_id = 18
                   order by last_update desc limit 1);
	$sth1 = $dbh->prepare($sql1);
	$sth1->execute();
	($exp{'esgf_link'}, $exp{'esgf_link_desc'}) = $sth1->fetchrow();
	$sth1->finish();

	# get CDG publication links, if it exists
	$sql1 = qq(select count(link), link, description
                   from t2j_links
                   where case_id = $exp{'case_id'} and process_id = 20
                   order by last_update desc limit 1);
	$sth1 = $dbh->prepare($sql1);
	$sth1->execute();
	($exp{'cdg_link_count'}, $exp{'cdg_link'}, $exp{'cdg_link_desc'}) = $sth1->fetchrow();
	$sth1->finish();

	# TODO - add more publication types as they become available

	# get diags for each process
	my @diags;
	for my $proc (@process) {
	    $sql1 = qq(select p.description as process, j.link, j.description,
                   DATE_FORMAT(j.last_update, '%Y-%m-%d') as last_update
                   from t2_process as p, t2j_links as j where
                   j.case_id = $exp{'case_id'} and
                   j.process_id = p.id and
                   j.process_id = $proc
                   order by last_update desc
                   limit 1);
	    $sth1 = $dbh->prepare($sql1);
	    $sth1->execute();
	    while(my $ref1 = $sth1->fetchrow_hashref())
	    {
		my %diag;
		$diag{'process'} = $ref1->{'process'};
		$diag{'link'} = $ref1->{'link'};
		$diag{'description'} = $ref1->{'description'};
		$diag{'last_update'} = $ref1->{'last_update'};
		push (@diags, \%diag);
	    }
	    $sth1->finish();
	}

	$exp{'diags'} = \@diags;
	push(@exps, \%exp);
    }            
    $sth->finish();
    return @exps;
}
