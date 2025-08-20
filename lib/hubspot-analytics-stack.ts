import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as path from "path";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as glue from "aws-cdk-lib/aws-glue";
import * as iam from "aws-cdk-lib/aws-iam";

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

    const pipelinesDimFn = new lambda.DockerImageFunction(
      this,
      "PipelinesDimFn",
      {
        code: activitiesCode,
        memorySize: 1024,
        timeout: cdk.Duration.minutes(5),
        environment: {
          S3_BUCKET: bucket.bucketName,
          HUBSPOT_SECRET_ARN: hubspotSecret.secretArn,
          TASK: "pipelines_dim",
        },
      }
    );

    const contactsDimFn = new lambda.DockerImageFunction(
      this,
      "ContactsDimFn",
      {
        code: activitiesCode,
        memorySize: 1536,
        timeout: cdk.Duration.minutes(10),
        environment: {
          S3_BUCKET: bucket.bucketName,
          HUBSPOT_SECRET_ARN: hubspotSecret.secretArn,
          START_DATE: process.env.START_DATE ?? "2025-01-01",
          TASK: "contacts_dim",
        },
      }
    );

    bucket.grantReadWrite(dealsFn);
    bucket.grantReadWrite(activitiesFn);
    bucket.grantReadWrite(contactsFn);
    bucket.grantReadWrite(ownersDimFn);
    bucket.grantReadWrite(companiesDimFn);
    bucket.grantReadWrite(pipelinesDimFn);
    bucket.grantReadWrite(contactsDimFn);

    hubspotSecret.grantRead(dealsFn);
    hubspotSecret.grantRead(activitiesFn);
    hubspotSecret.grantRead(contactsFn);
    hubspotSecret.grantRead(ownersDimFn);
    hubspotSecret.grantRead(companiesDimFn);
    hubspotSecret.grantRead(pipelinesDimFn);
    hubspotSecret.grantRead(contactsDimFn);

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

    new cdk.CfnOutput(this, "OwnersDimFunctionName", {
      value: ownersDimFn.functionName,
    });
    new cdk.CfnOutput(this, "CompaniesDimFunctionName", {
      value: companiesDimFn.functionName,
    });
    new cdk.CfnOutput(this, "PipelinesDimFunctionName", {
      value: pipelinesDimFn.functionName,
    });
    new cdk.CfnOutput(this, "ContactsDimFunctionNameDim", {
      value: contactsDimFn.functionName,
    });
    new cdk.CfnOutput(this, "HubspotSecretName", {
      value: hubspotSecret.secretName,
    });

    // === Glue Data Catalog: Database + Crawler to discover curated schemas ===
    const glueDbName = "hubspot_datalake";
    new glue.CfnDatabase(this, "HubspotDataLakeDb", {
      catalogId: cdk.Stack.of(this).account,
      databaseInput: { name: glueDbName },
    });

    const crawlerRole = new iam.Role(this, "GlueCrawlerRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSGlueServiceRole"
        ),
      ],
    });
    bucket.grantRead(crawlerRole); // allow crawler to read curated data

    const curatedPath = `s3://${bucket.bucketName}/curated/`;
    const crawler = new glue.CfnCrawler(this, "CuratedCrawler", {
      name: "hubspot-curated-crawler",
      role: crawlerRole.roleArn,
      databaseName: glueDbName,
      targets: { s3Targets: [{ path: curatedPath }] },
      schemaChangePolicy: {
        updateBehavior: "UPDATE_IN_DATABASE",
        deleteBehavior: "LOG",
      },
      recrawlPolicy: { recrawlBehavior: "CRAWL_EVERYTHING" },
      // optional schedule example: cron(0 2 * * ? *)
      // schedule: { scheduleExpression: "cron(0 2 * * ? *)" },
    });

    // separate crawler for dimension tables under s3://.../dim/
    const dimTargets = [
      { path: `s3://${bucket.bucketName}/dim/owners/` },
      { path: `s3://${bucket.bucketName}/dim/stage/` },
      { path: `s3://${bucket.bucketName}/dim/companies/` },
      { path: `s3://${bucket.bucketName}/dim/contacts/` },
    ];
    const dimCrawler = new glue.CfnCrawler(this, "DimCrawler", {
      name: "hubspot-dim-crawler",
      role: crawlerRole.roleArn,
      databaseName: glueDbName,
      targets: { s3Targets: dimTargets },
      schemaChangePolicy: {
        updateBehavior: "UPDATE_IN_DATABASE",
        deleteBehavior: "LOG",
      },
      recrawlPolicy: { recrawlBehavior: "CRAWL_EVERYTHING" },
    });

    new cdk.CfnOutput(this, "GlueDatabaseName", { value: glueDbName });
    new cdk.CfnOutput(this, "GlueCrawlerName", {
      value: crawler.name || "hubspot-curated-crawler",
    });
    new cdk.CfnOutput(this, "GlueDimCrawlerName", {
      value: dimCrawler.name || "hubspot-dim-crawler",
    });
  }
}
