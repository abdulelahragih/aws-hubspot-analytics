import * as cdk from "aws-cdk-lib";
import {Construct} from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as path from "path";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as glue from "aws-cdk-lib/aws-glue";
import * as iam from "aws-cdk-lib/aws-iam";
import * as stepfunctions from "aws-cdk-lib/aws-stepfunctions";
import * as stepfunctionstasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subscriptions from "aws-cdk-lib/aws-sns-subscriptions";
import * as scheduler from "aws-cdk-lib/aws-scheduler";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as ssm from "aws-cdk-lib/aws-ssm";

// Environment configuration interface
interface EnvironmentConfig {
    readonly environment: string;
    readonly isProduction: boolean;
    readonly snsEmailRecipients: string[];
}

// Load environment configuration
function loadEnvironmentConfig(): EnvironmentConfig {
    const environment =
        process.env.CDK_ENVIRONMENT || process.env.NODE_ENV || "dev";
    const isProduction =
        environment.toLowerCase() === "prod" ||
        environment.toLowerCase() === "production";

    // SNS email recipients from environment variables
    const snsEmails = process.env.SNS_EMAIL_RECIPIENTS
        ? process.env.SNS_EMAIL_RECIPIENTS.split(",").map((email) => email.trim())
        : [];

    return {
        environment,
        isProduction,
        snsEmailRecipients: snsEmails
    };
}

function getUtcDateOneYearAgo(): string {
    const now = new Date();
    now.setUTCFullYear(now.getUTCFullYear() - 1);
    return now.toISOString().slice(0, 10);
}

export class HubspotAnalyticsStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        const envConfig = loadEnvironmentConfig();

        // S3 bucket for data lake
        const bucket = new s3.Bucket(this, "DataLakeBucket", {
            encryption: s3.BucketEncryption.S3_MANAGED,
            blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
            enforceSSL: true,
            removalPolicy: envConfig.isProduction
                ? cdk.RemovalPolicy.RETAIN
                : cdk.RemovalPolicy.DESTROY,
            autoDeleteObjects: !envConfig.isProduction,
        });

        // Secret for HubSpot token (seed with env if provided, else let AWS generate a placeholder)
        const hubspotSecret = new secretsmanager.Secret(this, "HubspotToken", {
            secretName: "HubspotToken",
            secretStringValue: cdk.SecretValue.unsafePlainText("NotSetYet"),
        });

        // DynamoDB table for tracking sync state
        const syncStateTable = new dynamodb.Table(this, "SyncStateTable", {
            tableName: `hubspot-sync-state`,
            partitionKey: {
                name: "object_type",
                type: dynamodb.AttributeType.STRING,
            },
            billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
            removalPolicy: envConfig.isProduction
                ? cdk.RemovalPolicy.RETAIN
                : cdk.RemovalPolicy.DESTROY
        });

        // Parameter Store parameter for incremental sync toggle
        const incrementalSyncParameter = new ssm.StringParameter(
            this,
            "IncrementalSyncEnabled",
            {
                parameterName: `/hubspot-analytics/incremental-sync-enabled`,
                stringValue: "false", // Start with disabled by default
                description:
                    "Enable or disable incremental sync for HubSpot data ingestion",
                tier: ssm.ParameterTier.STANDARD,
            }
        );

        // Python Lambda packaged as a Docker container image
        const dockerPath = path.resolve(__dirname, "..", "lambda");
        const dockerCode = lambda.DockerImageCode.fromImageAsset(dockerPath, {
            cmd: ["app.handler"],
        });

        const lambdasSharedEnv = {
            S3_BUCKET: bucket.bucketName,
            HUBSPOT_SECRET_ARN: hubspotSecret.secretArn,
            SYNC_STATE_TABLE: syncStateTable.tableName,
            INCREMENTAL_SYNC_PARAMETER: incrementalSyncParameter.parameterName,
        }

        const dealsFn = new lambda.DockerImageFunction(this, "DealsFn", {
            code: dockerCode,
            memorySize: 2048,
            timeout: cdk.Duration.minutes(15),
            environment: {
                ...lambdasSharedEnv,
                TASK: "deals",
            },
        });

        const activitiesFn = new lambda.DockerImageFunction(
            this,
            "ActivitiesIngestFn",
            {
                code: dockerCode,
                memorySize: 2048,
                timeout: cdk.Duration.minutes(15),
                environment: {
                    ...lambdasSharedEnv,
                    START_DATE: process.env.START_DATE ?? getUtcDateOneYearAgo(),
                    TASK: "activities",
                },
            }
        );

        const ownersFn = new lambda.DockerImageFunction(this, "OwnersFn", {
            code: dockerCode,
            memorySize: 1024,
            timeout: cdk.Duration.minutes(5),
            environment: {
                ...lambdasSharedEnv,
                TASK: "owners",
            },
        });

        const companiesFn = new lambda.DockerImageFunction(this, "CompaniesFn", {
            code: dockerCode,
            memorySize: 1536,
            timeout: cdk.Duration.minutes(10),
            environment: {
                ...lambdasSharedEnv,
                TASK: "companies",
            },
        });

        const pipelinesFn = new lambda.DockerImageFunction(
            this,
            "PipelinesFn",
            {
                code: dockerCode,
                memorySize: 1024,
                timeout: cdk.Duration.minutes(5),
                environment: {
                    ...lambdasSharedEnv,
                    TASK: "pipelines",
                },
            }
        );

        const contactsFn = new lambda.DockerImageFunction(this, "ContactsFn", {
            code: dockerCode,
            memorySize: 2048,
            timeout: cdk.Duration.minutes(15),
            environment: {
                S3_BUCKET: bucket.bucketName,
                HUBSPOT_SECRET_ARN: hubspotSecret.secretArn,
                TASK: "contacts",
                SYNC_STATE_TABLE: syncStateTable.tableName,
                INCREMENTAL_SYNC_PARAMETER: incrementalSyncParameter.parameterName,
            },
        });

        bucket.grantReadWrite(dealsFn);
        bucket.grantReadWrite(activitiesFn);
        bucket.grantReadWrite(contactsFn);
        bucket.grantReadWrite(ownersFn);
        bucket.grantReadWrite(companiesFn);
        bucket.grantReadWrite(pipelinesFn);

        hubspotSecret.grantRead(dealsFn);
        hubspotSecret.grantRead(activitiesFn);
        hubspotSecret.grantRead(contactsFn);
        hubspotSecret.grantRead(ownersFn);
        hubspotSecret.grantRead(companiesFn);
        hubspotSecret.grantRead(pipelinesFn);

        // Grant DynamoDB permissions for sync state tracking
        syncStateTable.grantReadWriteData(dealsFn);
        syncStateTable.grantReadWriteData(activitiesFn);
        syncStateTable.grantReadWriteData(contactsFn);
        syncStateTable.grantReadWriteData(companiesFn);

        // Grant Parameter Store read permissions
        incrementalSyncParameter.grantRead(dealsFn);
        incrementalSyncParameter.grantRead(activitiesFn);
        incrementalSyncParameter.grantRead(contactsFn);
        incrementalSyncParameter.grantRead(companiesFn);

        new cdk.CfnOutput(this, "BucketName", {value: bucket.bucketName});
        new cdk.CfnOutput(this, "DealsFunctionName", {
            value: dealsFn.functionName,
        });
        new cdk.CfnOutput(this, "ActivitiesFunctionName", {
            value: activitiesFn.functionName,
        });
        new cdk.CfnOutput(this, "ContactsFunctionName", {
            value: contactsFn.functionName,
        });

        new cdk.CfnOutput(this, "OwnersFunctionName", {
            value: ownersFn.functionName,
        });
        new cdk.CfnOutput(this, "CompaniesFunctionName", {
            value: companiesFn.functionName,
        });
        new cdk.CfnOutput(this, "PipelinesFunctionName", {
            value: pipelinesFn.functionName,
        });
        new cdk.CfnOutput(this, "HubspotSecretName", {
            value: hubspotSecret.secretName,
        });

        // === Glue Data Catalog: Database + Crawler to discover curated schemas ===
        const glueDbName = "hubspot_datalake";
        new glue.CfnDatabase(this, "HubspotDataLakeDb", {
            catalogId: cdk.Stack.of(this).account,
            databaseInput: {name: glueDbName},
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
            targets: {s3Targets: [{path: curatedPath}]},
            schemaChangePolicy: {
                updateBehavior: "UPDATE_IN_DATABASE",
                deleteBehavior: "LOG",
            },
            recrawlPolicy: {recrawlBehavior: "CRAWL_EVERYTHING"},
        });

        // separate crawler for dimension tables under s3://.../dim/
        const dimTargets = [
            {path: `s3://${bucket.bucketName}/dim/owners/`},
            {path: `s3://${bucket.bucketName}/dim/stage/`},
            {path: `s3://${bucket.bucketName}/dim/companies/`},
            {path: `s3://${bucket.bucketName}/dim/contacts/`},
        ];
        const dimCrawler = new glue.CfnCrawler(this, "DimCrawler", {
            name: "hubspot-dim-crawler",
            role: crawlerRole.roleArn,
            databaseName: glueDbName,
            targets: {s3Targets: dimTargets},
            schemaChangePolicy: {
                updateBehavior: "UPDATE_IN_DATABASE",
                deleteBehavior: "LOG",
            },
            recrawlPolicy: {recrawlBehavior: "CRAWL_EVERYTHING"},
        });

        // === SNS Topic for email notifications ===
        const notificationTopic = new sns.Topic(
            this,
            "HubspotDataIngestNotifications",
            {
                displayName: "HubSpot Data Ingest Notifications",
                topicName: "hubspot-data-ingest",
            }
        );

        // Add email subscriptions based on environment configuration
        envConfig.snsEmailRecipients.forEach((email, _) => {
            notificationTopic.addSubscription(
                new subscriptions.EmailSubscription(email)
            );
        });

        // === Step Functions State Machine for orchestrating lambda execution ===
        const stepFunctionRole = new iam.Role(this, "StepFunctionRole", {
            assumedBy: new iam.ServicePrincipal("states.amazonaws.com"),
        });

        // Grant permissions to invoke lambdas and publish to SNS
        dealsFn.grantInvoke(stepFunctionRole);
        activitiesFn.grantInvoke(stepFunctionRole);
        contactsFn.grantInvoke(stepFunctionRole);
        ownersFn.grantInvoke(stepFunctionRole);
        companiesFn.grantInvoke(stepFunctionRole);
        pipelinesFn.grantInvoke(stepFunctionRole);
        notificationTopic.grantPublish(stepFunctionRole);

        // Grant permissions to start Glue crawlers
        stepFunctionRole.addToPolicy(
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                    "glue:StartCrawler",
                    "glue:GetCrawler",
                    "glue:GetCrawlerMetrics",
                ],
                resources: [
                    `arn:aws:glue:${this.region}:${this.account}:crawler/${crawler.name}`,
                    `arn:aws:glue:${this.region}:${this.account}:crawler/${dimCrawler.name}`,
                ],
            })
        );

        // Define the Step Functions tasks
        const wait1 = new stepfunctions.Wait(this, "Wait1", {
            time: stepfunctions.WaitTime.duration(cdk.Duration.seconds(10)),
        });
        const wait2 = new stepfunctions.Wait(this, "Wait2", {
            time: stepfunctions.WaitTime.duration(cdk.Duration.seconds(10)),
        });
        const wait3 = new stepfunctions.Wait(this, "Wait3", {
            time: stepfunctions.WaitTime.duration(cdk.Duration.seconds(10)),
        });
        const wait4 = new stepfunctions.Wait(this, "Wait4", {
            time: stepfunctions.WaitTime.duration(cdk.Duration.seconds(10)),
        });
        const wait5 = new stepfunctions.Wait(this, "Wait5", {
            time: stepfunctions.WaitTime.duration(cdk.Duration.seconds(10)),
        });
        const wait6 = new stepfunctions.Wait(this, "Wait6", {
            time: stepfunctions.WaitTime.duration(cdk.Duration.seconds(10)),
        });
        const wait7 = new stepfunctions.Wait(this, "Wait7", {
            time: stepfunctions.WaitTime.duration(cdk.Duration.seconds(10)),
        });

        const invokeDealsFn = new stepfunctionstasks.LambdaInvoke(
            this,
            "InvokeDealsFunction",
            {
                lambdaFunction: dealsFn,
                resultPath: "$.dealsResult",
            }
        );

        const invokeActivitiesFn = new stepfunctionstasks.LambdaInvoke(
            this,
            "InvokeActivitiesFunction",
            {
                lambdaFunction: activitiesFn,
                resultPath: "$.activitiesResult",
            }
        );

        const invokeOwnersFn = new stepfunctionstasks.LambdaInvoke(
            this,
            "InvokeOwnersFunction",
            {
                lambdaFunction: ownersFn,
                resultPath: "$.ownersResult",
            }
        );

        const invokeCompaniesFn = new stepfunctionstasks.LambdaInvoke(
            this,
            "InvokeCompaniesFunction",
            {
                lambdaFunction: companiesFn,
                resultPath: "$.companiesResult",
            }
        );

        const invokePipelinesFn = new stepfunctionstasks.LambdaInvoke(
            this,
            "InvokePipelinesFunction",
            {
                lambdaFunction: pipelinesFn,
                resultPath: "$.pipelinesResult",
            }
        );

        const invokeContactsFn = new stepfunctionstasks.LambdaInvoke(
            this,
            "InvokeContactsFunction",
            {
                lambdaFunction: contactsFn,
                resultPath: "$.contactsResult",
            }
        );

        // Now define Glue Crawler tasks with proper references
        const startCuratedCrawler = new stepfunctionstasks.CallAwsService(
            this,
            "StartCuratedCrawler",
            {
                service: "glue",
                action: "startCrawler",
                parameters: {Name: crawler.name!},
                resultPath: "$.curatedCrawlerResult",
                iamResources: [
                    `arn:aws:glue:${this.region}:${this.account}:crawler/${crawler.name}`,
                ],
            }
        );

        const startDimCrawler = new stepfunctionstasks.CallAwsService(
            this,
            "StartDimCrawler",
            {
                service: "glue",
                action: "startCrawler",
                parameters: {Name: dimCrawler.name!},
                resultPath: "$.dimCrawlerResult",
                iamResources: [
                    `arn:aws:glue:${this.region}:${this.account}:crawler/${dimCrawler.name}`,
                ],
            }
        );

        // Now define the complete workflow with proper crawler references
        // Success notification
        const successNotification = new stepfunctionstasks.SnsPublish(
            this,
            "SendSuccessNotification",
            {
                topic: notificationTopic,
                subject: "HubSpot Data Ingest - Success",
                message: stepfunctions.TaskInput.fromJsonPathAt("$.message"),
            }
        );

        const failureNotification = new stepfunctionstasks.SnsPublish(
            this,
            "SendFailureNotification",
            {
                topic: notificationTopic,
                subject: "HubSpot Data Ingest - Failure",
                message: stepfunctions.TaskInput.fromJsonPathAt(
                    "$.errorDetails.message"
                ),
            }
        );

        // Define the workflow chain
        const prepareSuccessMessage = new stepfunctions.Pass(
            this,
            "PrepareSuccessMessage",
            {
                result: stepfunctions.Result.fromObject({
                    message:
                        "All HubSpot data ingestion tasks and crawlers completed successfully!",
                }),
            }
        );

        const definition = invokeDealsFn
            .next(wait1)
            .next(invokeActivitiesFn)
            .next(wait2)
            .next(invokeOwnersFn)
            .next(wait3)
            .next(invokeCompaniesFn)
            .next(wait4)
            .next(invokePipelinesFn)
            .next(wait5)
            .next(invokeContactsFn)
            .next(wait6)
            .next(startCuratedCrawler)
            .next(wait7)
            .next(startDimCrawler)
            .next(prepareSuccessMessage)
            .next(successNotification);

        // Prepare error message with more details
        const prepareFailureMessage = new stepfunctions.Pass(
            this,
            "PrepareFailureMessage",
            {
                result: stepfunctions.Result.fromObject({
                    message:
                        "HubSpot data ingestion workflow failed. Check Step Functions execution logs for details.",
                }),
                resultPath: "$.errorDetails",
            }
        );

        // Create a Try/Catch pattern using Parallel state for top-level error handling
        const tryWorkflow = new stepfunctions.Parallel(this, "TryWorkflow", {
            resultPath: "$.workflowResult",
        });

        tryWorkflow.branch(definition);

        tryWorkflow.addCatch(prepareFailureMessage.next(failureNotification), {
            errors: [stepfunctions.Errors.ALL],
            resultPath: "$.error",
        });

        const stateMachine = new stepfunctions.StateMachine(
            this,
            "HubspotDataIngestWorkflow",
            {
                definitionBody: stepfunctions.DefinitionBody.fromChainable(tryWorkflow),
                role: stepFunctionRole,
                timeout: cdk.Duration.hours(2),
            }
        );

        // === EventBridge Scheduler for Sunday scheduling in Santiago timezone ===
        // Using EventBridge Scheduler with native timezone support
        const schedulerRole = new iam.Role(this, "SchedulerRole", {
            assumedBy: new iam.ServicePrincipal("scheduler.amazonaws.com"),
            inlinePolicies: {
                StateMachinePolicy: new iam.PolicyDocument({
                    statements: [
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: ["states:StartExecution"],
                            resources: [stateMachine.stateMachineArn],
                        }),
                    ],
                }),
            },
        });

        const weeklySchedule = new scheduler.CfnSchedule(
            this,
            "WeeklyHubspotIngestSchedule",
            {
                name: "hubspot-weekly-ingest",
                description: "Trigger HubSpot data ingestion workflow every Sunday at 5 AM Montevideo time",
                scheduleExpression: "cron(0 5 ? * SUN *)", // 5 AM every Sunday
                scheduleExpressionTimezone: "America/Montevideo",
                flexibleTimeWindow: {
                    mode: "OFF", // Execute exactly at scheduled time
                },
                target: {
                    arn: stateMachine.stateMachineArn,
                    roleArn: schedulerRole.roleArn,
                    input: JSON.stringify({
                        reason: "Weekly Sunday run",
                        scheduledExecution: true,
                        timezone: "America/Montevideo",
                        environment: envConfig.environment,
                    }),
                },
                state: envConfig.isProduction ? "ENABLED" : "DISABLED", // Disable scheduler in non-prod by default
            }
        );

        new cdk.CfnOutput(this, "GlueDatabaseName", {value: glueDbName});
        new cdk.CfnOutput(this, "GlueCrawlerName", {
            value: crawler.name || "hubspot-curated-crawler",
        });
        new cdk.CfnOutput(this, "GlueDimCrawlerName", {
            value: dimCrawler.name || "hubspot-dim-crawler",
        });

        // === Additional Outputs for the new infrastructure ===
        new cdk.CfnOutput(this, "StateMachineArn", {
            value: stateMachine.stateMachineArn,
            description:
                "Step Functions State Machine ARN for HubSpot data ingestion workflow",
        });

        new cdk.CfnOutput(this, "NotificationTopicArn", {
            value: notificationTopic.topicArn,
            description: "SNS Topic ARN for email notifications",
        });

        new cdk.CfnOutput(this, "ScheduleName", {
            value: weeklySchedule.name || weeklySchedule.attrArn,
            description:
                "EventBridge Scheduler name for weekly Sunday scheduling in Santiago timezone",
        });

        new cdk.CfnOutput(this, "SyncStateTableName", {
            value: syncStateTable.tableName,
            description: "DynamoDB table name for tracking sync state",
        });

        new cdk.CfnOutput(this, "IncrementalSyncParameterName", {
            value: incrementalSyncParameter.parameterName,
            description: "Parameter Store parameter name for incremental sync toggle",
        });
    }
}
