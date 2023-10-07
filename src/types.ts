import {SQSClient, Message} from "@aws-sdk/client-sqs";

export interface SqsOptions {
    queueUrl: string;
    attributeNames: string[];
    messageAttributeNames: string[];
    batchSize: number;
    visibilityTimeout: number;
    waitTimeSeconds: number;
    pollingWaitTimeMs: number;
    sqs?: SQSClient;
    region?: string;
    contentType?: string;

    handleMessage?(message : Message) : Promise<Message | void>;
    handleMessageBatch?(messages : Message[]) : Promise<Message[] | void>;
    handleMessageTimeout: number;
}