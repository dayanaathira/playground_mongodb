
const { createLogger, format, transports } = require('winston');
const { combine, timestamp, label, printf } = format;
require('winston-daily-rotate-file');

const logFormat = printf(({ level, message, timestamp }) => {
    return `${timestamp} - ${level} : ${message}`;
  });

const debugTransportToFile = new transports.DailyRotateFile({
  level: 'info',
  filename:'engine-%DATE%.log',	
  dirname: './logs/',
  datePattern: 'YYYY-MM-DD',
  zippedArchive: true,
  maxFiles: '14d',
  maxSize: '20m'
})

const errorTransportToFile = new transports.DailyRotateFile({
  level: 'error',
  filename:'engine-error-%DATE%.log',	
  dirname: './logs/',
  datePattern: 'YYYY-MM-DD',
  zippedArchive: true,
  maxFiles: '14d',
  maxSize: '20m'
})

let logger = createLogger({
    level: 'debug',
    format: combine(
        timestamp({format: "YYYY-MM-DD HH:mm:ss"}),
        format.colorize(),
        logFormat
      ),
    // defaultMeta: { service: 'user-service' },
    transports: [ 
      new transports.Console(),
      debugTransportToFile, // this is used on info level
      errorTransportToFile  // this is used on error level
    ],
  });
  

module.exports = logger 