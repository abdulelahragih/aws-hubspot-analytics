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
    const hubspotSecret = new secretsmanager.Secret(this, "HubspotToken", {
      secretName: "HubspotToken",
      secretStringValue: cdk.SecretValue.unsafePlainText("NotSetYet"),
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

    const contactsFn = new lambda.DockerImageFunction(
      this,
      "ContactsIngestFn",
      {
        code: activitiesCode,
        memorySize: 2048,
        timeout: cdk.Duration.minutes(15),
        environment: {
          S3_BUCKET: bucket.bucketName,
          HUBSPOT_SECRET_ARN: hubspotSecret.secretArn,
          START_DATE: process.env.START_DATE ?? "2025-01-01",
          TASK: "contacts",
        },
      }
    );

    const dealsRawFn = new lambda.DockerImageFunction(
      this,
      "DealsRawIngestFn",
      {
        code: dealsCode,
        memorySize: 2048,
        timeout: cdk.Duration.minutes(15),
        environment: {
          S3_BUCKET: bucket.bucketName,
          HUBSPOT_SECRET_ARN: hubspotSecret.secretArn,
          START_DATE: process.env.START_DATE ?? "2025-01-01",
          TASK: "deals_raw",
        },
      }
    );

    const ownersDimFn = new lambda.DockerImageFunction(this, "OwnersDimFn", {
      code: activitiesCode,
      memorySize: 1024,
      timeout: cdk.Duration.minutes(5),
      environment: {
        S3_BUCKET: bucket.bucketName,
        HUBSPOT_SECRET_ARN: hubspotSecret.secretArn,
        TASK: "owners_dim",
      },
    });

    const companiesDimFn = new lambda.DockerImageFunction(
      this,
      "CompaniesDimFn",
      {
        code: activitiesCode,
        memorySize: 1536,
        timeout: cdk.Duration.minutes(10),
        environment: {
          S3_BUCKET: bucket.bucketName,
          HUBSPOT_SECRET_ARN: hubspotSecret.secretArn,
          START_DATE: process.env.START_DATE ?? "2025-01-01",
          TASK: "companies",
        },
      }
    );

    bucket.grantReadWrite(dealsFn);
    bucket.grantReadWrite(activitiesFn);
    bucket.grantReadWrite(contactsFn);
    bucket.grantReadWrite(dealsRawFn);
    bucket.grantReadWrite(ownersDimFn);
    bucket.grantReadWrite(companiesDimFn);

    hubspotSecret.grantRead(dealsFn);
    hubspotSecret.grantRead(activitiesFn);
    hubspotSecret.grantRead(contactsFn);
    hubspotSecret.grantRead(dealsRawFn);
    hubspotSecret.grantRead(ownersDimFn);
    hubspotSecret.grantRead(companiesDimFn);

    new cdk.CfnOutput(this, "BucketName", { value: bucket.bucketName });
    new cdk.CfnOutput(this, "DealsFunctionName", {
      value: dealsFn.functionName,
    });
    new cdk.CfnOutput(this, "ActivitiesFunctionName", {
      value: activitiesFn.functionName,
    });
    new cdk.CfnOutput(this, "ContactsFunctionName", {
      value: contactsFn.functionName,
    });
    new cdk.CfnOutput(this, "DealsRawFunctionName", {
      value: dealsRawFn.functionName,
    });
    new cdk.CfnOutput(this, "OwnersDimFunctionName", {
      value: ownersDimFn.functionName,
    });
    new cdk.CfnOutput(this, "CompaniesDimFunctionName", {
      value: companiesDimFn.functionName,
    });
    new cdk.CfnOutput(this, "HubspotSecretName", {
      value: hubspotSecret.secretName,
    });
  }
}
