from tracking_engine import TrackingPipeline

summary_df = TrackingPipeline(
    storage="local",  # local, aws_s3, azure_blob, adls
).run()

