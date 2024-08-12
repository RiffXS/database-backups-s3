import './index.mjs'

import { CronJob } from 'cron'
import { promisify } from 'util'
import { exec as cp_exec } from 'child_process'
import { readFileSync } from 'fs'
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3'

const exec = promisify(cp_exec)

function loadConfig() {
  const requiredEnvars = [
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_S3_REGION',
    'AWS_S3_ENDPOINT',
    'AWS_S3_BUCKET',
  ]

  for (const key of requiredEnvars) {
    if (!process.env[key]) {
      throw new Error(`Variável de Ambiente ${key} é obrigatória`)
    }
  }

  return {
    aws: {
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      },
      region: process.env.AWS_S3_REGION,
      endpoint: process.env.AWS_S3_ENDPOINT,
      s3_bucket: process.env.AWS_S3_BUCKET,
    },
    databases: process.env.DATABASES ? process.env.DATABASES.split(",") : [],
    run_on_startup: process.env.RUN_ON_STARTUP === 'true' ? true : false,
    cron: process.env.CRON,
  }
}

const config = loadConfig()

const s3Client = new S3Client(config.aws)

async function processBackup() {
  if (config.databases.length === 0) {
    console.log('Nenhuma database definida.')
    return
  }

  for (const [index, databaseURI] of config.databases.entries()) {
    const databaseIteration = index + 1
    const databaseTotal = config.databases.length

    const url = new URL(databaseURI)
    const dbType = url.protocol.slice(0, -1); // remove trailing colon
    const dbName = url.pathname.substring(1); // extract db name from URL
    const dbHostname = url.hostname;
    const dbUser = url.username;
    const dbPassword = url.password;
    const dbPort = url.port;

    const date = new Date();
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    const hh = String(date.getHours()).padStart(2, '0');
    const min = String(date.getMinutes()).padStart(2, '0');
    const ss = String(date.getSeconds()).padStart(2, '0');
    const timestamp = `${yyyy}-${mm}-${dd}_${hh}:${min}:${ss}`;
    const filename = `backup-${dbType}-${timestamp}-${dbName}-${dbHostname}.tar.gz`;
    const filepath = `/tmp/${filename}`;

    console.log(`\n[${databaseIteration}/${databaseTotal}] ${dbType}/${dbName} Backup in progress...`);

    let dumpCommand;
    switch (dbType) {
      case 'postgresql':
        dumpCommand = `pg_dump "${databaseURI}" -F c > "${filepath}.dump"`;
        break;

      case 'mongodb':
        dumpCommand = `mongodump --uri="${databaseURI}" --archive="${filepath}.dump"`;
        break;

      case 'mysql':
        dumpCommand = `mysqldump -u ${dbUser} -p${dbPassword} -h ${dbHostname} -P ${dbPort} ${dbName} > "${filepath}.dump"`;
        break;

      default:
        console.log(`Tipo de database desconhecido: ${dbType}`);
        return;
    }

    try {
      // 1. Executar o comando de Dump
      await exec(dumpCommand)

      // 2. Comprimir o arquivo Dump
      await exec(`tar -czvf ${filepath} ${filepath}.dump`)

      // 3. Ler o arquivo compresso
      const data = readFileSync(filepath)
    
      // 4. Upload para o S3
      const params = {
        Bucket: config.aws.s3_bucket,
        Key: filename,
        Body: data,
      }

      const putCommand = new PutObjectCommand(params)
      await s3Client.send(putCommand)

      console.log(`✓ A database ${dbType} ${dbName} ${dbHostname} teve seu backup concluído com sucesso.`);
    } catch (error) {
      console.error(`Um erro ocorreu durante o processamento da database ${dbType} ${dbName}, host: ${dbHostname}: ${error}`);
    }
  }
}

if (config.cron) {
  const job = new CronJob(config.cron, processBackup)
  job.start()

  console.log(`Backup configurado para o agendamento Cron: ${config.cron}`)
}

if (config.run_on_startup) {
  console.log('run_on_startup habilitado, começando backup agora...')
  processBackup()
}
