use inc::Module::Install;
name 'AnyEvent-DataSift';
all_from 'lib/AnyEvent/DataSift.pm';

requires 'AnyEvent';
requires 'AnyEvent::HTTP';
requires 'AnyEvent::Util';
requires 'URI';
requires 'URI::Escape';
requires 'JSON';
requires 'YAML';
requires 'Carp';
requires 'Getopt::Long';

tests 't/*.t';
#author_tests 'xt';

install_script 'bin/datasift-stream';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
