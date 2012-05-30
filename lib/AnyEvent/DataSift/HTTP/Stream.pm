package AnyEvent::DataSift::HTTP::Stream;

use strict;
use 5.010;

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
		stream        => undef,
		timeout       => 300, #5m
		useragent     => 'DataSiftPerlConsumer/'.$AnyEvent::DataSift::VERSION,
		on_connect    => sub{},
		on_disconnect => sub{},
		on_data       => sub{},
		on_error      => sub{},
		on_tick       => sub{},
		@_
	};

	croak 'username required'   unless $self->{username};
	croak 'apikey required'     unless $self->{apikey};
	croak 'streamhash required' unless $self->{stream};
	$self->{auth} = $self->{username}.':'.$self->{apikey};

	bless $self, $class;
	$self->connect;
	$self;
}

sub _receive {
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

sub connect {
	my $self = shift;

	my $uri = URI->new( $self->{protocol}.'://'.$self->{host}.':'.$self->{port}.'/'.($self->{stream}||'') );
	$uri->query_form( $self->{params} ) if $self->{method} eq 'POST';

	$self->{watchstream} = http_request(
		$self->{method},
		$uri,
		headers => {
			Connection    => 'Keep-Alive',
			Host          => $self->{host},
			Authorization => $self->{auth},
			'User-Agent'  => $self->{useragent},
			($self->{method} eq 'POST'
				? ('Content-Type' => 'application/x-www-form-urlencoded')
				: ()
			),
		},
		timeout => $self->{timeout},
		on_header => sub {
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
					$self->_receive( $chunk );
				});
			};

			my $line_reader = sub {
				my($handle, $line) = @_;
				$self->_receive( $line );
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
