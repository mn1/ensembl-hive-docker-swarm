=pod 

=head1 NAME

Bio::EnsEMBL::Hive::Meadow::DockerSwarm

=head1 DESCRIPTION

    This is the implementation of Meadow for a Swarm or Docker Engines

=head1 LICENSE

    Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
    Copyright [2016-2017] EMBL-European Bioinformatics Institute

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and limitations under the License.

=head1 CONTACT

    Please subscribe to the Hive mailing list:  http://listserver.ebi.ac.uk/mailman/listinfo/ehive-users  to discuss Hive-related questions or to be notified of our updates

=cut


package Bio::EnsEMBL::Hive::Meadow::DockerSwarm;

use strict;
use warnings;
use Cwd ('cwd');
use Bio::EnsEMBL::Hive::Utils ('destringify', 'split_for_bash');

use base ('Bio::EnsEMBL::Hive::Meadow', 'Bio::EnsEMBL::Hive::Utils::RESTclient');


our $VERSION = '5.1';       # Semantic version of the Meadow interface:
                            #   change the Major version whenever an incompatible change is introduced,
                            #   change the Minor version whenever the interface is extended, but compatibility is retained.

sub construct_base_url {
    my $dma = $ENV{'DOCKER_MASTER_ADDR'};
    return $dma && "http://$dma/v1.30";
}


sub new {
    my $class   = shift @_;

    my $self    = $class->SUPER::new( @_ );                             # First construct a Meadow
    $self->base_url( $class->construct_base_url // '' );                # Then initialise the RESTclient extension
    $self->{_DOCKER_MASTER_ADDR} = $ENV{'DOCKER_MASTER_ADDR'};          # saves the location of the manager node

    return $self;
}


sub name {  # also called to check for availability
    my ($self) = @_;

    my $url = '';
    unless (ref($self)) {
        # Object instances have defined the base URL in the parent class
        $url = $self->construct_base_url;
        return undef unless $url;
    }
    $url .= '/swarm';

    my $swarm_attribs   = $self->GET( $url ) || {};
    
    if (ref $swarm_attribs ne 'HASH') {
        $swarm_attribs = JSON->new->decode( $swarm_attribs );
    }

    return $swarm_attribs->{'ID'};
}


sub _get_our_task_attribs {
    my ($self) = @_;

    my $cmd = q(cat /proc/self/cgroup | grep docker | sed -e s/\\\\//\\\\n/g | tail -1);
    my $container_prefix    = `$cmd`; chomp $container_prefix;

#     use Data::Dumper;
#     print "My container_prefix is: $container_prefix\n";

    my $tasks_list          = $self->GET( '/tasks' );
    my ($our_task_attribs)  = grep { ($_->{'Status'}{'ContainerStatus'}{'ContainerID'} || '') =~ /^${container_prefix}/ } @$tasks_list;

    #print "My task attribs: " . Dumper($our_task_attribs);
    return $our_task_attribs;
}


sub get_current_hostname {
    my ($self) = @_;

    my $nodes_list          = $self->GET( '/nodes' );
    my %node_id_2_ip        = map { ($_->{'ID'} => $_->{'Status'}{'Addr'}) } @$nodes_list;
    my $our_node_ip         = $node_id_2_ip{ $self->_get_our_task_attribs()->{'NodeID'} };

    return $our_node_ip;
}


sub get_current_worker_process_id {
    my ($self) = @_;

    my $our_task_id         = $self->_get_our_task_attribs()->{'ID'};

    return $our_task_id;
}


sub deregister_local_process {
    my $self = shift @_;
    # so that the LOCAL child processes don't think they belong to the DockerSwarm meadow
    delete $ENV{'DOCKER_MASTER_ADDR'};
}


sub status_of_all_our_workers { # returns an arrayref
    my ($self) = @_;

    # my $service_tasks_struct    = $self->GET( '/tasks?filters={"name":["' . $service_name . '"]}' );
    my $service_tasks_struct    = $self->GET( '/tasks' );

    my @status_list = ();
    foreach my $task_entry (@$service_tasks_struct) {
        my $slot        = $task_entry->{'Slot'};                # an index within the given service
        my $task_id     = $task_entry->{'ID'};
        my $prestatus   = lc $task_entry->{'Status'}{'State'};

        # Some statuses are explained at https://docs.docker.com/datacenter/ucp/2.2/guides/admin/monitor-and-troubleshoot/troubleshoot-task-state/
        my $status      = {
                            'new'       => 'PEND',
                            'pending'   => 'PEND',
                            'assigned'  => 'PEND',
                            'accepted'  => 'PEND',
                            'preparing' => 'RUN',
                            'starting'  => 'RUN',
                            'running'   => 'RUN',
                            'complete'  => 'DONE',
                            'shutdown'  => 'DONE',
                            'failed'    => 'EXIT',
                            'rejected'  => 'EXIT',
                            'orphaned'  => 'EXIT',
                        }->{$prestatus} || $prestatus;

        push @status_list, [ $task_id, 'docker_user', $status ];
    }

    return \@status_list;
}

sub submit_workers_return_meadow_pids {
    my ($self, $worker_cmd, $required_worker_count, $iteration, $rc_name, $rc_specific_submission_cmd_args, $submit_log_subdir) = @_;

    my $worker_cmd_components = [ split_for_bash($worker_cmd) ];

    my $job_array_common_name = $self->job_array_common_name($rc_name, $iteration);
    
    print "--> job_array_common_name = $job_array_common_name\n";

    # Name collision detection
    my $extra_suffix = 0;
    my $service_name = $job_array_common_name;
    
    (
        my $service_created_struct,
        my $json_output_string
    )
        = $self->GET( '/tasks?filters={"name":["' . $service_name . '"]}' );

    
    while (scalar(@$service_created_struct)) {
        $extra_suffix++;
        $service_name = "$job_array_common_name-$extra_suffix";
    }
    if ($extra_suffix) {
        warn "'$job_array_common_name' already used to name a service. Using '$service_name' instead.\n";
        $job_array_common_name = $service_name;
    }

    die "The image name for the ".$self->name." DockerSwarm meadow is not configured. Cannot submit jobs !" unless $self->config_get('ImageName');

    # If the resource description is missing, use 1 core
    my $default_resources = {
        'Reservations'  => {
            'NanoCPUs'  => 1000000000,
        },
    };
    
    # Looks like this:
    #
    # 'Resources' => {
    #     'Reservations' => {
    #         'NanoCPUs' => 1000000000,
    #         'MemoryBytes' => '34359738368'
    #     },
    #     'Limits' => {
    #         'NanoCPUs' => 1000000000,
    #         'MemoryBytes' => '34359738368'
    #     }
    # }
    #
    my $resources = destringify($rc_specific_submission_cmd_args);

    if (
        ref $resources eq 'HASH' 
        && exists $resources->{Reservations} 
        && exists $resources->{Reservations}->{MemoryBytes}
    ) {
        my $x = $resources->{Reservations}->{MemoryBytes};
        $x += 0;
        $resources->{Reservations}->{MemoryBytes} = $x;
    
        $x = $resources->{Limits}->{MemoryBytes};
        $x += 0;
        $resources->{Limits}->{MemoryBytes} = $x;
    }
    
    if (
        ref $resources eq 'HASH' 
        && exists $resources->{Reservations} 
        && exists $resources->{Reservations}->{NanoCPUs}
    ) {
        my $x = $resources->{Reservations}->{NanoCPUs};
        $x += 0;
        $resources->{Reservations}->{NanoCPUs} = $x;
    
        $x = $resources->{Limits}->{NanoCPUs};
        $x += 0;
        $resources->{Limits}->{NanoCPUs} = $x;
    }

    my $service_create_data = {
        'Name'          => $job_array_common_name,      # NB: service names in DockerSwarm have to be unique!
        'TaskTemplate'  => {
            'ContainerSpec' => {
                'Image'     => $self->config_get('ImageName'),
                'Args'      => $worker_cmd_components,
                'Mounts'    => $self->config_get('Mounts'),
                'Env'       => [
                                # Propagate these to the workers
                               "DOCKER_MASTER_ADDR=$self->{'_DOCKER_MASTER_ADDR'}",
                               "_EHIVE_HIDDEN_PASS=$ENV{'_EHIVE_HIDDEN_PASS'}",
                ],
            },
            # NOTE: By default, docker alway keeps logs. Should we disable them here
            #       $submit_log_subdir has been set ? There are no options to redirect
            #       the logs, so the option's value would be ignored.
            #'LogDriver' => {
                #'Name'      => 'none',
            #},
            'Resources'     => $resources || $default_resources,
            'RestartPolicy' => {
                'Condition' => 'none',
            },
        },
        'Mode'          => {
            'Replicated'    => {
                'Replicas'  => int($required_worker_count),
            },
        },
    };
    
    use Data::Dumper;
    print "\nSubmitting workers:\n";
    print Dumper($service_create_data);
    
    (
        my $service_created_struct,
        my $json_output_string
    )
        = $self->POST( '/services/create', $service_create_data );

    my $no_services_submitted 
        = 
            ( ref $service_created_struct eq 'HASH' ) 
            && 
            ( exists $service_created_struct->{message} )
    ;
    
    if ($no_services_submitted) {
    
        warn "No service was submitted!";
        warn "The response was:";
        warn Dumper($json_output_string);
        return ();
    
    }

    sleep(5);
    
    # filters='{"name":["probemapping-Hive-default-24_1"]}'
    my $service_tasks_list_url = q(/tasks?filters={"name":[") . $job_array_common_name . q("]});
    print "--> service_tasks_list_url = $service_tasks_list_url\n";
    (
        my $service_tasks_list,
        my $json_output_string
    )
        = $self->GET( $service_tasks_list_url );

    print "\nservice_tasks_list:\n";
    print Dumper($service_tasks_list);
    
    my @children_task_ids       = map { $_->{'ID'} } @$service_tasks_list;

    print "\nchildren_task_ids:\n";
    print Dumper(\@children_task_ids);

    return \@children_task_ids;
}

# Overwrites the method in Bio::EnsEMBL::Hive::Utils::RESTclient
# Returns the json_output_string for generating better error messages.
#
sub run_curl_capture_and_parse_result {
    my ($self, $curl_cmd, $raw_json_output_filename) = @_;

    my $url = join(' ',@$curl_cmd);
    
    open(my $curl_output_fh, "-|", @$curl_cmd) || die "Could not run '". $url ."' : $!, $?";
    my $json_output_string  = <$curl_output_fh>;
    close $curl_output_fh;

    if($raw_json_output_filename) {
        open(my $fh, '>', $raw_json_output_filename);
        print $fh $json_output_string;
        close $fh;
    }
    
    my $perl_struct;
    
    if ($json_output_string) {
        $perl_struct = JSON->new->decode( $json_output_string );
    } else {
        warn("Got no reply for: $url");
    }

#     warn("json_output_string = $json_output_string");
#     warn("perl_struct = " . Dumper($perl_struct));
    
    return $perl_struct, $json_output_string;
}

sub run_on_host {   # Overrides Meadow::run_on_host ; not supported yet - it's just a placeholder to block the base class' functionality
    my ($self, $meadow_host, $meadow_user, $command) = @_;

    return undef;
}

1;
