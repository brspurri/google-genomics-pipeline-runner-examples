#!/usr/bin/python

#
# The MIT License (MIT)
# Copyright (c) 2016 Brett Spurrier
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#

"""Python sample demonstrating use of the Google Genomics Pipelines API.

https://github.com/googlegenomics/pipelines-api-examples

This sample demonstrates running Variant Effecrt Predictor (http://www.htslib.org/) over one
or more input VCF files stored in Google Cloud Storage.

This sample demonstrates running the pipeline in an "ephemeral" manner;
no call to pipelines.create() is necessary. No pipeline is persisted
in the pipelines list.

Usage:
  * python vep.py \
      --project <project-id> \
      --zones <gce-zones> \
      --disk-size <size-in-gb> \
      --vcd <gcs-input-path-to-vcf> \
      --output <gcs-output-path> \
      --logging <gcs-logging-path> \
      --poll-interval <interval-in-seconds>

Where the poll-interval is optional (default is no polling).

Users will typically want to restrict the Compute Engine zones to avoid Cloud
Storage egress charges. This script supports a short-hand pattern-matching
for specifying zones, such as:

  --zones "*"                # All zones
  --zones "us-*"             # All US zones
  --zones "us-central1-*"    # All us-central1 zones

an explicit list may be specified, space-separated:
  --zones us-central1-a us-central1-b
"""
import os
import sys

from mando import command, main

from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build

# Set the global scope variables
CREDENTIALS = GoogleCredentials.get_application_default()


@command('vep')
def vep(project=None,
        vcf=list(),
        disk_size=None,
        memory=1,
        storage_logging=None,
        storage_output=None,
        input_directory='input',
        output_directory='output',
        docker_image=None,
        run_name=None,
        poll_interval=0):
    """
    Run Emsembl Variant Effect Predictor (VEP) on Google Genomics Pipeline API
    """

    # Specify a default run name
    if not run_name:
        run_name = 'vep'

    # If no docker image is specified, then default to Google Container Repository
    if not docker_image:
        docker_image = 'gcr.io/{project}/vep/85'.format(project=project)

    # Set the mount points and input and out folders (on the VM)
    mount_point = '/mnt/data'
    input_mount_point = '{}/{}'.format(mount_point, input_directory)
    output_mount_point = '{}/{}'.format(mount_point, output_directory)
    sys.stdout.write('Mounting: {}\n'.format(input_mount_point))
    sys.stdout.write('Mounting: {}\n'.format(output_mount_point))

    # Set the name of the disk
    disk_name = 'datadisk'

    # Build the samtools index command
    cmd = ('mkdir -p /mnt/data/output && '
           'find /mnt/data/input && '
           'for f in $(/bin/ls /mnt/data/input); do '
           '  echo ${f}; '
           '  perl /tools/vep/ensembl-tools-release-85/scripts/variant_effect_predictor/variant_effect_predictor.pl '
           '      --species homo_sapiens '
           '      --input_file ${f} '
           '      --force --cache '
           '      --dir_cache /mnt/data/input/ '
           '      --offline --cache_version 85 '
           'done')

    # Create the genomics service
    service = build('genomics', 'v1alpha2', credentials=CREDENTIALS)

    # Create the pipeline
    pipeline = {

        # Establish the ephemeral pipeline
        'ephemeralPipeline': {

            # Project properties
            'projectId': project,
            'name': run_name,
            'description': 'Run samtools on one or more files via Google Genomics',

            # Resources
            'resources': {

                # Create a data disk that is attached to the VM and destroyed when the
                # pipeline terminates.
                'disks': [{
                    'name': disk_name,
                    'autoDelete': True,
                    'mountPoint': mount_point,
                }],
            },

            # Specify the Docker image to use along with the command
            'docker': {

                # Docker image name
                'imageName': docker_image,

                # Command to run
                'cmd': cmd,
            },

            # Copy the passed input files to the VM's input_folder on disk_name fromGS
            'inputParameters': [{
                'name': 'inputFile{}'.format(i),
                'description': 'Input file for: {run_name}'.format(run_name=run_name),
                'localCopy': {
                    'path': input_directory + '/',
                    'disk': disk_name
                }
                } for i in range(len(files))],

            # Copy the processed output files from the VM's input_folder on disk_name to GS
            'outputParameters': [{
                'name': 'outputPath',
                'description': 'Cloud Storage path for where to samtools output',
                'localCopy': {
                    'path': '{output_folder}/*'.format(output_folder=output_directory),
                    'disk': disk_name
                }
            }]
        },

        # Set the resources
        'pipelineArgs': {
            'projectId': project,

            # Override the resources needed for this pipeline
            'resources': {

                # Set the memory
                'minimumRamGb': memory,

                # Expand any zone short-hand patterns
                # 'zones': defaults.get_zones(zones),

                # For the data disk, specify the size
                'disks': [{
                    'name': disk_name,
                    'sizeGb': disk_size,
                }]
            },

            # Map the input files to the input file keys
            'inputs': {
                'inputFile{}'.format(i): f for i, f in enumerate(files)
            },

            # Pass the user-specified Cloud Storage destination path of the samtools output
            'outputs': {
                'outputPath': storage_output
            },

            # Pass the user-specified Cloud Storage destination for pipeline logging
            'logging': {
                'gcsPath': storage_logging
            }
        }
    }

    # Run the pipeline
    operation = service.pipelines().run(body=pipeline).execute()

    # # Emit the result of the pipeline run submission
    # pp = pprint.PrettyPrinter(indent=2)
    # pp.pprint(operation)
    #
    # # If requested - poll until the operation reaches completion state ("done: true")
    # if args.poll_interval > 0:
    #   completed_op = poller.poll(service, operation, args.poll_interval)
    #   pp.pprint(completed_op)


if __name__ == "__main__":
    main()

