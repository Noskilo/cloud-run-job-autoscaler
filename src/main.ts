import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { AppService } from './app.service';

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);

  const appService = app.get(AppService);

  const num = await appService.getNumUndeliverdMessages(
    'cloud-run-job-test-sub',
  );
  console.log(`Number of undelivered messages: ${num}`);

  const newParallelism = Math.min(Math.max(1, Math.round(num / 2)), 20);
  await appService.modifyJobParallelism(newParallelism);

  if (num === 0) {
    await appService.pauseScheduler('cloud-run-job-test-scheduler-trigger');
  } else {
    await appService.resumeScheduler('cloud-run-job-test-scheduler-trigger');
  }

  await app.close();
}
bootstrap();
