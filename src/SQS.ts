import {
  SQSClient,
  ChangeMessageVisibilityBatchCommand,
  ChangeMessageVisibilityCommand,
  DeleteMessageBatchCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  ReceiveMessageCommandInput,
  ReceiveMessageCommandOutput,
  Message
} from '@aws-sdk/client-sqs';
import {SqsOptions} from './types'

export class SQS {
  private queueUrl: string;
  private handleMessage: (message: Message) => Promise<Message | void>;
  private handleMessageBatch: (messages: Message[]) => Promise<Message[] | void>;
  private handleMessageTimeout: number;
  private attributeNames: string[];
  private messageAttributeNames: string[];
  private batchSize: number;
  private visibilityTimeout: number;
  private waitTimeSeconds: number;
  private pollingWaitTimeMs: number;
  private sqs: SQSClient;
  private contentType: string;
  private isRunning: boolean;

  /**
   * queueUrl: 대기열 URL
   * attributeNames: 받을 메세지 속성 설정
   * batchSize: 한번에 받을 최대 메시지 개수
   * MessageAttributeNames: 받을 메시지 추가 속성
   * VisibilityTimeout: 초 단위로 된 보기 제한 시간 - default: 30s
   * WaitTimeSeconds: 짧은 폴링, 긴 폴링 사용 설정
   * handleMessage: 메세지 처리 Function
   * handleMessageBatch: 메세지 Bulk 처리 Function (handleMessage 보다 우선하여 사용)
   * handleMessageTimeout: 처리 만료 시간
   * pollingWaitTimeMs: 다음 메세지 가져오기 전 반드시 기다리는 시간 (Ms)
   * @param {object} options
   */
  constructor(options : SqsOptions) {
    this.queueUrl = options.queueUrl;
    this.handleMessage = options.handleMessage;
    this.handleMessageBatch = options.handleMessageBatch;
    this.handleMessageTimeout = options.handleMessageTimeout;
    this.attributeNames = options.attributeNames || [];
    this.messageAttributeNames = options.messageAttributeNames || [];
    this.batchSize = options.batchSize || 1;
    this.visibilityTimeout = options.visibilityTimeout || 30;
    this.waitTimeSeconds = options.waitTimeSeconds ?? 20;
    this.pollingWaitTimeMs = options.pollingWaitTimeMs ?? 0;
    this.sqs = options.sqs || new SQSClient({
      region: options.region || process.env.AWS_REGION || 'ap-northeast-2'
    });
    this.contentType = options.contentType || 'json';
    this.isRunning = false;
  }

  /**
   * Message poll
   * @returns {Promise<void>}
   */
  async poll() : Promise<void> {
    this.isRunning = true;
    try {
      const response = await this.receiveMessage({
        QueueUrl: this.queueUrl,
        AttributeNames: this.attributeNames,
        MessageAttributeNames: this.messageAttributeNames,
        MaxNumberOfMessages: this.batchSize,
        WaitTimeSeconds: this.waitTimeSeconds,
        VisibilityTimeout: this.visibilityTimeout
      });
      await this.handleSqsResponse(response);
    } catch (e) {
      console.error(e.message);
    } finally {
      // next message poll
      this.poll();
    }
  }
  async receiveMessage(params: ReceiveMessageCommandInput): Promise<ReceiveMessageCommandOutput> {
    try {
      return await this.sqs.send(new ReceiveMessageCommand(params));
    } catch (e) {
      console.error(e.message);
      throw new Error(e);
    }
  }
  async handleSqsResponse(response: ReceiveMessageCommandOutput): Promise<void> {
    if (response.Messages && response.Messages.length > 0) {
      if (this.handleMessageBatch) {
        await this.processMessageBatch(response.Messages);
      } else {
        await Promise.all(response.Messages.map(async message => {
          this.processMessage(message);
        }));
      }
    }
  }
  async processMessage(message) {
    try {
      const result = await this.executeHandler(message);
      if (result?.MessageId === message.MessageId) {
        // success
        await this.deleteMessage(message);
      }
    } catch (e) {
      // error
      console.error(e.message);
      await this.changeVisibilityTimeout(message, this.visibilityTimeout);
    }
  }
  async processMessageBatch(messages) {
    try {
      const results = await this.executeBatchHandler(messages);
      if (results?.length > 0) {
        // success
        await this.deleteMessageBatch(messages);
      }
    } catch (e) {
      // error
      console.error(e.message);
      await this.changeVisibilityTimeoutBatch(messages, this.visibilityTimeout);
    }
  }
  async executeHandler(message) {
    let handleMessageTimeoutId = undefined;

    try {
      let result;
      const job = JSON.parse(JSON.stringify(message));
      if (this.contentType === 'json') {
        job.Body = JSON.parse(job.Body);
      }
      if (this.handleMessageTimeout) {
        const pending = new Promise((_, reject) => {
          handleMessageTimeoutId = setTimeout(() => {
            reject(new Error('executeHandler timeout'));
          }, this.handleMessageTimeout);
        });
        result = await Promise.race([this.handleMessage(job), pending]);
      } else {
        result = await this.handleMessage(job);
      }

      return result instanceof Object ? result : message;
    } catch (e) {
      if (e instanceof Error) {
        e.message = `Unexpected message handler failure: ${e.message}`;
      }
      throw e;
    } finally {
      if (handleMessageTimeoutId) {
        clearTimeout(handleMessageTimeoutId);
      }
    }
  }
  async executeBatchHandler(messages) {
    try {
      const jobs = JSON.parse(JSON.stringify(messages));
      if (this.contentType === 'json') {
        jobs.forEach(job => job.Body = JSON.parse(job.Body));
      }
      const result = await this.handleMessageBatch(jobs);

      return result instanceof Object ? result : messages;
    } catch (e) {
      if (e instanceof Error) {
        e.message = `Unexpected message handler failure: ${e.message}`;
      }
      throw e;
    }
  }
  async deleteMessage(message) {
    const deleteParams = {
      QueueUrl: this.queueUrl,
      ReceiptHandle: message.ReceiptHandle
    };
    await this.sqs.send(new DeleteMessageCommand(deleteParams));
  }
  async deleteMessageBatch(messages) {
    const deleteParams = {
      QueueUrl: this.queueUrl,
      Entries: messages.map(message => ({
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle
      }))
    };

    await this.sqs.send(new DeleteMessageBatchCommand(deleteParams));
  }
  async changeVisibilityTimeout(message, timeout) {
    const input = {
      QueueUrl: this.queueUrl,
      ReceiptHandle: message.ReceiptHandle,
      VisibilityTimeout: timeout
    };
    return this.sqs.send(new ChangeMessageVisibilityCommand(input));
  }
  async changeVisibilityTimeoutBatch(messages, timeout) {
    const params = {
      QueueUrl: this.queueUrl,
      Entries: messages.map(message => ({
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: timeout
      }))
    };
    return this.sqs.send(new ChangeMessageVisibilityBatchCommand(params));
  }
}