import subprocess
from google.cloud import storage
from raster_loader import BigQueryConnection


INPUT = "/Users/aaronfraint/Downloads/radd_alert_gt.tif"
OUTPUT = "/Users/aaronfraint/Downloads/radd_alert_gt_warped.tif"
BUCKET = "aaron-deforestation-etl"
FILE_TO_PROCESS = "radd_alert_gt.tif"


class Raster:
    def __init__():
        pass

    def download_from_bucket(self):

        print("Downloading data from bucket")

        storage_client = storage.Client()

        bucket = storage_client.bucket()

        for b in bucket.list_blobs():
            if b.name == FILE_TO_PROCESS:
                blob = bucket.blob(b.name)
                blob.download_to_filename(INPUT)

    def warp_tif(self):
        print("Warping tiff")

        cmd = f"""   \
            gdalwarp \
                -of COG \
                -co TILING_SCHEME=GoogleMapsCompatible \
                -co COMPRESS=DEFLATE \
                -co OVERVIEWS=NONE \
                -co ADD_ALPHA=NO \
                -co RESAMPLING=NEAREST \
                -co BLOCKSIZE=512 \
                {INPUT} \
                {OUTPUT}
        """
        result = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE)

    def send_to_bq(self):
        print("Exporting to BQ")

        connection = BigQueryConnection("cartodb-gcp-solutions-eng-team")

        connection.upload_raster(
            file_path=OUTPUT,
            fqn="cartodb-gcp-solutions-eng-team.AFRAINT.radd_geotiff_v2",
        )

    def process_file(self):
        self.download_from_bucket()
        self.warp_tif()
        self.send_to_bq()
