import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as path from "path";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";

export class HubspotAnalyticsStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 bucket for data lake (dev-friendly removal policy)
    const bucket = new s3.Bucket(this, "DataLakeBucket", {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // Secret for HubSpot token (seed with env if provided, else let AWS generate a placeholder)
    const hubspotSecret = process.env.HUBSPOT_TOKEN
      ? new secretsmanager.Secret(this, "HubspotToken", {
          secretName: "HubspotToken",
          secretStringValue: cdk.SecretValue.unsafePlainText(
            process.env.HUBSPOT_TOKEN
          ),
        })
      : new secretsmanager.Secret(this, "HubspotToken", {
          secretName: "HubspotToken",
        });

    // Python Lambda packaged as a Docker container image
    const dockerPath = path.resolve(__dirname, "..", "lambda");
    const dealsCode = lambda.DockerImageCode.fromImageAsset(dockerPath, {
      cmd: ["app.handler"],
    });
    const activitiesCode = lambda.DockerImageCode.fromImageAsset(dockerPath, {
      cmd: ["app.handler"],
    });

    const dealsFn = new lambda.DockerImageFunction(this, "DealsIngestFn", {
      code: dealsCode,
      memorySize: 2048,
      timeout: cdk.Duration.minutes(15),
      environment: {
        S3_BUCKET: bucket.bucketName,
        HUBSPOT_SECRET_ARN: hubspotSecret.secretArn,
        START_DATE: process.env.START_DATE ?? "2025-01-01",
        TASK: "deals",
      },
    });

    const activitiesFn = new lambda.DockerImageFunction(
      this,
      "ActivitiesIngestFn",
      {
        code: activitiesCode,
        memorySize: 2048,
        timeout: cdk.Duration.minutes(15),
        environment: {
          S3_BUCKET: bucket.bucketName,
          HUBSPOT_SECRET_ARN: hubspotSecret.secretArn,
          START_DATE: process.env.START_DATE ?? "2025-01-01",
          TASK: "activities",
        },
      }
    );

    bucket.grantReadWrite(dealsFn);
    bucket.grantReadWrite(activitiesFn);
    hubspotSecret.grantRead(dealsFn);
    hubspotSecret.grantRead(activitiesFn);

    new cdk.CfnOutput(this, "BucketName", { value: bucket.bucketName });
    new cdk.CfnOutput(this, "DealsFunctionName", {
      value: dealsFn.functionName,
    });
    new cdk.CfnOutput(this, "ActivitiesFunctionName", {
      value: activitiesFn.functionName,
    });
    new cdk.CfnOutput(this, "HubspotSecretName", {
      value: hubspotSecret.secretName,
    });
  }
}
