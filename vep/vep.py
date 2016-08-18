import os
import sys
import json

from mando import command, main
from oauth2client.client import GoogleCredentials
from pipeline_runner import Pipeline


@command('vep')
def vep(cache_bunde=None,
        cache_version=None,
        species=None,
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
            --cache_bundle gs://brettspurrier-geomes/homo_sapiens/vep/homo_sampiens_vep_cache.tar.gz
            --cache_version 85
            --species homo_sapiens
            --vcf gs://brettspurrier-test/mytest_vcf.vcf
            --storage_output gs://brettspurrier-data
            --storage_logging gs://brettspurrier-logs

    :param cache_bunde: Google Storage path for the VEP cache archive.
    :param cache_version: Ensembl VEP cache_bunde version.
    :param species: Species prefix for the cache.
    :param vcf: List of Google Storage paths for the input VCF files.
    :param project: Google Project name.
    :param storage_output: Google Storage bucket for the output data.
    :param storage_logging: Google Storage bucket for the log files.
    :param extra_command_flags: Extra flags to pass to VEP.
    """

    # Get your Google Credentials from your local system
    credentials = GoogleCredentials.get_application_default()

    # Make sure a Google project name has been passed
    if not project:
        sys.stderr.write(('Please provide a Google Project name by passing the '
                          '`--project {MyGoogleProject}` argument.'))
        sys.exit(1)

    # Make sure a cache bundle genome has been passed
    if not cache_bunde:
        sys.stderr.write(('Please provide a species cache by passing the '
                          '`--cache_bunde gs://{bucket}/{vep_cache}.tar.gz` argument. '
                          'Example cache: '
                          'ftp://ftp.ensembl.org/pub/release-85/variation/VEP/homo_sapiens_vep_85_GRCh38.tar.gz'))
        sys.exit(1)

    # Make sure a cache version has been passed
    if not cache_version:
        sys.stderr.write(('Please provide a species cache version by passing the '
                          '`--cache_version {cache_version}` argument.'))
        sys.exit(1)

    # Make sure a species has been passed
    if not species:
        sys.stderr.write(('Please provide a species cache version by passing the '
                          '`--species {species}` argument.'))
        sys.exit(1)

    # Make sure a VCF file(s)have been passed
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
        'vep_test_pipeline',
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
    p.add_input(disk, cache_bunde)

    # ===================================================
    #  Create the VEP execution command
    #    Alternatively, you may construct a command.sh and
    #    simply execute that script on pipeline start.
    #  The current command below only supports running a
    #    single VCF file, however, this can easily be
    #    modified to loop over several input VCFs.
    # ---------------------------------------------------
    #  Create the output directory, cd into it and run VEP.
    #  ------
    #    mkdir -p /mnt/data/output
    #    cd /mnt/data/output
    #    tar zxvf /mnt/data/input/homo_sapiens_cache.tar.gz -C /mnt/data/input/
    #    perl variant_effect_predictor.pl \
    #       --species homo_sapiens \
    #       --input_file example_GRCh38.vcf \
    #       --force \
    #       --cache \
    #       --cache_version 85 \
    #       --dir_cache /cache/ \
    #       --offline
    #       --json
    #  ------
    cmd = 'mkdir -p {mount}/output; '.format(mount=mount)
    cmd += 'tar zxvf {mount}/input/{species_cache_bunde} -C {mount}/input/; '.format(
        mount=mount,
        species_cache_bunde=os.path.basename(cache_bunde)
    )
    cmd += 'cd {mount}/input; '.format(mount=mount)
    cmd += 'ls -lah'
    cmd += ('perl variant_effect_predictor.pl '
            '   --species {species} '
            '   --input_file {vcf} '
            '   --force '
            '   --dir_cache /mnt/data/input/ --cache --cache_version 85 --offline '
            '   --output_file /mnt/data/output/{output_file}.json --json').format(
        species=species,
        vcf=vcf,
        output_file=os.path.basename(os.path.splitext(vcf)[0] + '.json')
    )

    # Add any extra flags if they are passed.
    #   Specific plugins can be passed here.
    if extra_command_flags:
        cmd += ' {}'.format(extra_command_flags)
    p.command = cmd

    # Build the VEP pipeline configuration
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