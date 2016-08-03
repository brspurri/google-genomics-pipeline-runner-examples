
Usage
=====

Building the docker image
-------------------------
To build:
```
   docker build -t bwa:0.7.15 .
```

To tag your build for pushing to GCR:
```
   docker tag bwa:0.7.15 gcr.io/your-project-id/bwa/0.7.15
```

To push the image to GCR:
```
   gcloud docker push gcr.io/{PROJECT}/bwa/0.7.15
```


Run the Pipeline on Google Genomics Infastructure
-------------------------------------------------

Run the following command, only change the bucket names and references as appropriate:
```
    python bwa.py bwa \
        --project genomics-986 \
        --fastq gs://brettspurrier-test/input_fastq_a.fastq.gz \
        --fastq gs://brettspurrier-test/input_fastq_b.fastq.gz \
        --storage_output gs://brettspurrier-data \
        --storage_logging gs://brettspurrier-logs \
        --reference_bundle gs://brettspurrier-genomes/homo_sapiens/grch38/reference_bwa.tar.gz \
        --reference genome.fa
```

