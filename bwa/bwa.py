import os
import sys
import json

from mando import command, main
from oauth2client.client import GoogleCredentials
from pipeline_runner import Pipeline


@command('bwa')
def bwa_mem(reference_bundle=None,
            reference=None,
            fastq=list(),
            project=None,
            storage_output=None,
            storage_logging=None,
            extra_command_flags=None):
    """
    BWA MEM runner for Google Genomics Pipeline API.
    Executes: `bwa mem ref.fa fastq_a.fastq.gz fastq_b.fastq.gz > aligned.sam`

    Example usage:

        python bwa.py bwa \
            --project genomics-986
            --fastq gs://brettspurrier-test/input_fastq_a.fastq.gz
            --fastq gs://brettspurrier-test/input_fastq_b.fastq.gz
            --storage_output gs://brettspurrier-data
            --storage_logging gs://brettspurrier-logs
            --reference_bundle gs://brettspurrier-genomes/homo_sapiens/grch38/reference_bwa.tar.gz
            --reference genome.fa

    :param reference_bundle: Google Storage path for the reference genome files.
    :param reference: Reference genome basename. Fasta basename once extracted from reference_bundle.
    :param fastq: List of Google Storage paths for the input fastq files.
    :param project: Google Project name.
    :param storage_output: Google Storage bucket for the output data.
    :param storage_logging: Google Storage bucket for the log files.
    :param extra_command_flags: Extra flags to pass to BWA MEM.
    """

    # Get your Google Credentials from your local system
    credentials = GoogleCredentials.get_application_default()

    # Make sure a Google project name has been passed
    if not project:
        sys.stderr.write(('Please provide a Google Project name by passing the '
                          '`--project {MyGoogleProject}` argument.'))
        sys.exit(1)

    # Make sure a reference bundle genome has been passed
    if not reference_bundle:
        sys.stderr.write(('Please provide a reference fasta by passing the '
                          '`--reference_bundle gs://{bucket}/reference_bwa.tar.gz` argument.'))
        sys.exit(1)

    # Make sure a reference genome has been passed
    if not reference:
        sys.stderr.write(('Please provide a reference fasta by passing the '
                          '`--reference reference.fa` argument.'))
        sys.exit(1)

    # Make sure a fastq file(s)have been passed
    if not fastq:
        sys.stderr.write(('Please provide input fastq fasta by passing the '
                          '`--fastq gs://{bucket}/fastq_1.fq` argument.'))
        sys.exit(1)

    # Make sure a Google Storage bucket has been passed to store the output
    if not storage_output:
        sys.stderr.write(('Please provide a Google Storage output bucket path by passing the '
                          '`--output_storage gs://{MyOutputBucket}` argument.'))
        sys.exit(1)

    # Make sure a Google Storage bucket has been passed to store the logs
    if not storage_output:
        sys.stderr.write(('Please provide a Google Storage log bucket path by passing the '
                          '`--output_logging gs://{MyLogsBucket}` argument.'))
        sys.exit(1)

    # ===================================================
    #  Dynamically build the BWA pipeline configuration
    # ---------------------------------------------------
    cores = 2
    memory = 16
    p = Pipeline(
        credentials,
        project,
        'bwa_test_pipeline',
        'gcr.io/{project}/bwa/0.7.15'.format(project=project),
        storage_output,
        storage_logging,
        cores=cores,
        memory=memory
    )

    # Mount a 50GB disk to /mnt/data
    disk = 'datadisk'
    mount = '/mnt/data'
    p.add_disk(disk, mount, size=50)

    # Instruct the pipeline to transfer all the input fastqs to the mounted disk
    for fq in fastq:
        p.add_input(disk, fq)

    # Instruct the pipeline to transfer all the reference files components to the mounted disk
    p.add_input(disk, '{reference_bundle}'.format(reference_bundle=reference_bundle))

    # ===================================================
    #  Create the BWA execution command
    #    Alternatively, you may construct a run_me.sh and
    #    simply execute that script on pipeline start.
    # ---------------------------------------------------
    #  Create the output directory, cd into it and run BWA mem.
    #  ------
    #    mkdir -p /mnt/data/output
    #    cd /mnt/data/output
    #    bwa mem genome.fa fastq1.gz fastq2.gz {extra_command_flags}
    #  ------
    cmd = 'mkdir -p {mount}/output; '.format(mount=mount)
    cmd += 'tar zxvf {mount}/input/{reference_bundle} -C {mount}/input/; '.format(
        mount=mount,
        reference_bundle=os.path.basename(reference_bundle)
    )
    cmd += 'cd {mount}/input; '.format(mount=mount)
    cmd += 'bwa mem -t {cores} {reference} {fastq} > {mount}/{output}; '.format(
        cores=cores,
        reference=os.path.basename(reference),
        fastq=' '.join([mount + '/input/' + os.path.basename(f) for f in fastq]),
        mount=mount,
        output='output/aligned.sam'
    )

    # Add any extra flags if they are passed
    if extra_command_flags:
        cmd += ' {}'.format(extra_command_flags)
    p.command = cmd

    # Build the BWA pipeline configuration
    p.build()

    # ===================================================
    #  Execute the pipeline
    # ---------------------------------------------------
    operation = p.run()

    # Printout the response to SDTOUT
    sys.stdout.write('Pipeline Response:\n')
    sys.stdout.write(json.dumps(operation, indent=4, sort_keys=True))
    sys.stdout.write('\n')

if __name__ == "__main__":
    main()