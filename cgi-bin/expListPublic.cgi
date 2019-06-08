#!/usr/bin/env perl
#
use warnings;
use strict;
use CGI qw(:standard);
use DBI;
use DBD::mysql;
use Time::localtime;
use HTML::Entities;
use CGI::Carp qw(fatalsToBrowser warningsToBrowser); 
use Template;
use lib qw(.);
use lib "/home/www/html/csegdb/lib";
use config;
use lib "/home/www/html/exp2-public/lib";
use expPublic;

#------
# main 
#------
$ENV{PATH} = '';

my $req = CGI->new;
my %config = &getconfig;
my $dbname = $config{'dbname'};
my $dbhost = $config{'dbhost'};
my $dbuser = $config{'dbuser'};
my $dbpasswd = $config{'dbpassword'};
my $dsn = $config{'dsn'};

# initialize global item hash
my %item = ();
my $dbh = DBI->connect($dsn, $dbuser, $dbpasswd) or die "unable to connect to db: $DBI::errstr";

my @exps = getPublicExps($dbh);

my $vars = {
    exps => \@exps
};
	
print $req->header();
my $tmplFile = '../templates/showPublicExps.tmpl';

my $template = Template->new({
    ENCODING => 'utf8',
    RELATIVE => 1,
    INCLUDE_PATH => '/home/www/html/includes:/home/www/html/exp2-public/templates',
			     });

$template->process($tmplFile, $vars) || die ("Problem processing $tmplFile, ", $template->error());

$dbh->disconnect;
exit;
