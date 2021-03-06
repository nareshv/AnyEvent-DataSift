package AnyEvent::DataSift::Stream::HTTP;

use 5.010;
use strict;
use warnings;

use Encode;
use AnyEvent;
use AnyEvent::HTTP;
use AnyEvent::Util;
use URI;
use URI::Escape;
use Carp;
use JSON;

sub new {
	my $class = shift;
	my $self = {
		host          => 'stream.datasift.com',
		port          => 80,
		method        => 'GET',
		params        => {},
		protocol      => 'http',
		username      => undef,
		apikey        => undef,
		hash          => undef,
		timeout       => 300, #5m
		useragent     => 'DataSiftPerlConsumer/'.$AnyEvent::DataSift::VERSION,
		on_connect    => sub{},
		on_disconnect => sub{},
		on_data       => sub{},
		on_error      => sub{},
		on_tick       => sub{},
		@_
	};

	croak 'username required' unless $self->{username};
	croak 'apikey required'   unless $self->{apikey};
	croak 'hash required'     unless $self->{hash};
	$self->{auth} = $self->{username}.':'.$self->{apikey};

	bless $self, $class;
	$self->connect;
	$self;
}

sub connect {
	my $self = shift;

	sub receive {
		my( $self, $raw ) = @_;
		return unless $raw;
		my $data = eval { decode_json( decode_utf8( $raw ) ) };
		unless( $data ){
			warn "invalid json data: $raw";
			return;
		}
		if( $data->{tick} ){
			$self->{on_tick}->($data);
		} else {
			$self->{on_data}->($data);
		}
	}

	my $headers = {
		Connection    => 'Keep-Alive',
		Host          => $self->{host},
		Authorization => $self->{auth},
		'User-Agent'  => $self->{useragent},
	};
	my $body;
	my $uri = URI->new( $self->{protocol}.'://'.$self->{host}.':'.$self->{port}.'/'.$self->{hash} );
	if( $self->{method} eq 'POST' ){
		$body = join '&', 
			map $_.'='.URI::Escape::uri_escape($self->{params}{$_}),
			keys %{$self->{params}};
		$headers->{'Content-Type'} = 'application/x-www-form-urlencoded';
	} else {
		$uri->query_form( $self->{params} );
	}

	$self->{watchstream} = http_request(
		$self->{method} => $uri,
		headers         => $headers,
		body            => $body,
		timeout         => $self->{timeout},
		on_header       => sub {
			my($headers) = @_;
			if ($headers->{Status} ne '200') {
				$self->{on_error}->(
					'header',
					status => $headers->{Status},
					reason => $headers->{Reason},
				);
				return;
			}
			return 1;
		},
		want_body_handle => 1,
		sub {
			my($handle, $headers) = @_;
			return unless $handle;

			my $chunk_reader = sub {
				my($handle, $line) = @_;
				return unless $line;
				#read chunk size
				$line =~ /^([0-9a-fA-F]+)/ or croak 'bad chunk (incorrect length) -['.$line.']-';
				my $len = hex $1;
				#read chunk
				$handle->push_read(chunk => $len, sub {
					my($handle, $chunk) = @_;
					$handle->push_read(line => sub {
					 	length $_[1] and die 'bad chunk (missing last empty line)';
					});
					receive( $self, $chunk );
				});
			};

			my $line_reader = sub {
				my($handle, $line) = @_;
				receive( $self, $line );
			};

			$handle->on_error(sub {
				undef $handle;
				$self->{on_error}->('data',$_[2]);
			});

			$handle->on_eof(sub {
				undef $handle;
				$self->{on_disconnect}->(@_);
			});

			if (($headers->{'transfer-encoding'} || '') =~ /\bchunked\b/i) {
				$handle->on_read(sub {
					my ($handle) = @_;
					$handle->push_read(line => $chunk_reader);
				});
			} else {
				$handle->on_read(sub {
					my ($handle) = @_;
					$handle->push_read(line => $line_reader);
				});
			}

			$self->{watchguard} = AnyEvent::Util::guard {
				$handle->destroy if $handle;
			};

		}
	);

	$self;
}

1;
__END__
