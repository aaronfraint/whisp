import ee


def get_data_from_ee_into_bucket():
    from datasets.wur_radd_alerts import wur_radd_alerts_prep

    x = wur_radd_alerts_prep(8)

    roi = [
        [-75.652631002410104, -9.08404921149643],
        [-74.550990107182386, -9.08404921149643],
        [-74.550990107182386, -8.074604252450298],
        [-75.652631002410104, -8.074604252450298],
        [-75.652631002410104, -9.08404921149643],
    ]

    task = ee.batch.Export.image.toCloudStorage(
        image=x,
        description="image_export_cog",
        bucket="aaron-deforestation-etl",
        fileNamePrefix="radd_alert_gt",
        region=roi,
        scale=10,
        crs="EPSG:4326",
        maxPixels=1e13,
        fileFormat="GeoTIFF",
        formatOptions={"cloudOptimized": True},
    )

    task.start()

    return task


if __name__ == "__main__":
    task = get_data_from_ee_into_bucket()
