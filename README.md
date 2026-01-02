# TDR Aggregate Processing

Lambda to handle the aggregate processing of a loaded transfer from a client, and integrate the loaded metadata into the TDR transfer workflow.

## Modules

### Aggregate Processing

Wrapper for handling all the transfer's metadata sidecar S3 objects once client completes the uploading of the objects to the designated S3 bucket.

### Asset Processing

Processes the individual metadata sidecar S3 objects based on the source of the asset:
* reads the metadata sidecar S3 object from the provided S3 object key;
* parses the metadata and checks valid;
* persists the metadata if valid;

### Orchestration

Orchestrates the processing of the transfer once the individual assets have been processed. 

Holds business logic for triggering post-load operations within the TDR workflow.

## Error Handling

Errors generated during the running of the lambda are logged via an "error handler" to ensure consistent handling.

## Running Locally

1. Provide AWS credentials with permissions to read objects from the S3 bucket where the metadata sidecars have been uploaded to;
2. Add the relevant environment variables. See `application.conf`;
3. Update the `AggregateProcessingLambdaRunner.scala` with the AWS S3 source bucket and object key prefix for the metadata sidecars to be processed;
4. Run the `AggregateProcessingLambdaRunner`
