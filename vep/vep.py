import os
import sys
import json

from mando import command, main
from oauth2client.client import GoogleCredentials
from pipeline_runner import Pipeline


@command('vep')
def vep(species_cache_bunde=None,
        species_cache_version=None,
        vcf=list(),
        project=None,
        storage_output=None,
        storage_logging=None,
        extra_command_flags=None):
    """
    Ensembl Variant Effect Predictor (VEP) runner for Google Genomics Pipeline API.
    Executes: `variant_effect_predictor.pl ...`

    Example usage:

        python vep.py vep \
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
    if not species_cache_bunde:
        sys.stderr.write(('Please provide a species cache by passing the '
                          '`--species_cache_bunde gs://{bucket}/{vep_cache}.tar.gz` argument. '
                          'Example cache: '
                          'ftp://ftp.ensembl.org/pub/release-85/variation/VEP/homo_sapiens_vep_85_GRCh38.tar.gz'))
        sys.exit(1)

    # Make sure a reference genome has been passed
    if not species_cache_version:
        sys.stderr.write(('Please provide a species cache version by passing the '
                          '`--species_cache_version {cache_version}` argument.'))
        sys.exit(1)

    # Make sure a fastq file(s)have been passed
    if not vcf:
        sys.stderr.write(('Please provide input VCF file by passing the '
                          '`--vcf gs://{bucket}/{my_file}.vcf` argument.'))
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
    #  Dynamically build the VEP pipeline configuration
    # ---------------------------------------------------
    cores = 4
    memory = 16
    p = Pipeline(
        credentials,
        project,
        'bwa_test_pipeline',
        'gcr.io/{project}/vep/85'.format(project=project),
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
    for v in vcf:
        p.add_input(disk, v)

    # Instruct the pipeline to transfer all the cache files to the mounted disk
    p.add_input(disk, species_cache_bunde)

    # ===================================================
    #  Create the VEP execution command
    #    Alternatively, you may construct a command.sh and
    #    simply execute that script on pipeline start.
    # ---------------------------------------------------
    #  Create the output directory, cd into it and run BWA mem.
    #  ------
    #    mkdir -p /mnt/data/output
    #    cd /mnt/data/output
    #    perl variant_effect_predictor.pl \
    #       --species homo_sapiens \
    #       --input_file example_GRCh38.vcf \
    #       --force \
    #       --cache \
    #       --cache_version 85 \
    #       --dir_cache /cache/ \
    #       --offline
    #  perl /tools/vep/ensembl-tools-release-85/scripts/variant_effect_predictor/variant_effect_predictor.pl --species homo_sapiens --input_file /tools/vep/ensembl-tools-release-85/scripts/variant_effect_predictor/example_GRCh38.vcf --force --cache --cache_version 85  --dir_cache /mnt/data/input/ --offline --output_file /mnt/data/input/test.json --json
    #  ------
    cmd = 'mkdir -p {mount}/output; '.format(mount=mount)
    cmd += 'tar zxvf {mount}/input/{species_cache_bunde} -C {mount}/input/; '.format(
        mount=mount,
        species_cache_bunde=os.path.basename(species_cache_bunde)
    )
    cmd += 'cd {mount}/input; '.format(mount=mount)
    cmd += 'ls -lah'

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