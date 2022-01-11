These ingestion scripts are reliant on output from running scripts from the `msf-flow` repository.

These scripts assume:
- Have read/write access to a Postgres database.
- Have Python and Bash installed

For every script that interacts with the database, you must fill in the parameters inside the script itself to match your database:
`DB_ENDPOINT = "localhost"
DB_PORT = 5432
DB_USER = ""
DB_PASSWD = ""
DB_NAME = ""`

### How to ingest CSV files made from msf-flow into the database
#### For plumes:
- python ingest_permian_plume_csv_to_sql.py -d [your plumes CSV file name here]
#### For sources:
- python ingest_permian_source_csv_to_sql.py -d [your sources CSV file name here]

### How to ingest a list of image files (of TIFF format) into the database
#### NOTE: The filenames of these images MUST match an existing `Candidate ID` already in the database
- cd [directory with all of the geotiffs. The names of the geotiffs need to match the candidate ID ingested by the scripts above.] ; 
find . | grep ".tif" > list_of_image_filenames.txt
- (Skip the first step if you already have a list of images in a text file)
- sh ingest_plumes_from_file_to_sql.sh your-text-file-of-image-names.txt

After these steps are completed, your data should be uploaded to your Postgres database such that it is visible through the Methane web portal.
