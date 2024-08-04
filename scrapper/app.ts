import { SQSEvent } from 'aws-lambda';
import { z, ZodError } from 'zod';
import { AxiosHttpGateway } from './adapters/axios-http-gateway';
import { CheerioNfeParser } from './adapters/cheerio-nfe-parser';
import { SnsMessageBroker } from './adapters/sns-message-broker';
import { HttpGateway } from './bondaries/http-gateway';
import { NfeParser } from './bondaries/nfe-parser';
import { MessageBroker } from './bondaries/message-broker';
import { ExtractAndPublishNfeUseCase } from './use-cases/extract-and-publish-nfe-data-use-case';

export async function lambdaHandler(event: SQSEvent): Promise<void> {
    const recordSchema = z.array(
        z.object({
            url: z.string().url(),
        }),
    );

    try {
        const records = recordSchema.parse(
            event.Records.map((record) => ({
                url: JSON.parse(record.body).url,
            })),
        );

        const httpGateway: HttpGateway = new AxiosHttpGateway();
        const nfeParser: NfeParser = new CheerioNfeParser();
        const messageBroker: MessageBroker = new SnsMessageBroker();

        const extractAndPublishNfeDataUseCase = new ExtractAndPublishNfeUseCase(httpGateway, nfeParser, messageBroker);

        for (const record of records) {
            await extractAndPublishNfeDataUseCase.execute({ url: record.url });
        }
        return;
    } catch (error: unknown) {
        if (error instanceof ZodError) {
            console.log(error.format());
            return;
        }
        if (error instanceof Error) {
            console.log(error.message);
            return;
        }
        console.log(error);
        return;
    }
}
