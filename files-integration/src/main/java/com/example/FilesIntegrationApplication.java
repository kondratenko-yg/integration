package com.example;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ImageBanner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.core.io.FileSystemResource;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.ftp.dsl.Ftp;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.ReflectionUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class FilesIntegrationApplication {
    
    private String ascii = "ascii";

    @Bean
    DefaultFtpSessionFactory ftpFileSessionFactory(
            @Value("${ftp.port:21210}") int port,
            @Value("${ftp.username:jlong}") String username,
            @Value("${ftp.password:spring}") String pw) {
        DefaultFtpSessionFactory ftpSessionFactory = new DefaultFtpSessionFactory();
        ftpSessionFactory.setPort(port);
        ftpSessionFactory.setPassword(pw);
        ftpSessionFactory.setUsername(username);
        return ftpSessionFactory;
    }

    @Bean
    public Map<String, Object> producerNonTransactionalConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "id");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return props;
    }

    @Bean
    public ProducerFactory<String, String> producerNonTransactionalAbstractMessageFactory() {
        return new DefaultKafkaProducerFactory<>(producerNonTransactionalConfigs());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerNonTransactionalAbstractMessageFactory());
        template.setMessageConverter(new StringJsonMessageConverter());
        return template;
    }

    @Bean
    Exchange exchange() {
        return ExchangeBuilder.directExchange(this.ascii).durable(false).build();
    }

    @Bean
    Queue queue() {
        return QueueBuilder.durable(this.ascii).build();
    }

    @Bean
    Binding binding() {
        return BindingBuilder.bind(this.queue())
                .to(this.exchange())
                .with(this.ascii)
                .noargs();
    }

    @Bean
    IntegrationFlow kafka(KafkaTemplate<String, String> kafkaTemplate) {
        return IntegrationFlows.from(producerChannel())
                .handle(message -> kafkaTemplate.send(this.ascii, (String) message.getHeaders().get(FileHeaders.FILENAME)))
                .get();
    }


    @Bean
    IntegrationFlow files(@Value("${input-directory:C:/Users/kondratenko_yg/Desktop/in}") File in,
                          Environment environment) {

        GenericTransformer<File, Message<String>> fileStringGenericTransformer = (File source) -> {

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 PrintStream printStream = new PrintStream(baos)) {
                ImageBanner imageBanner = new ImageBanner(new FileSystemResource(source));
                imageBanner.printBanner(environment, getClass(), printStream);
                return MessageBuilder.withPayload(new String(baos.toByteArray()))
                        .setHeader(FileHeaders.FILENAME, source.getAbsoluteFile().getName())
                        .build();
            } catch (IOException e) {
                ReflectionUtils.rethrowRuntimeException(e);
            }
            return null;
        };

        return IntegrationFlows
                .from(Files.inboundAdapter(in).autoCreateDirectory(true).preventDuplicates(false).patternFilter("*.jpg"),
                        poller -> poller.poller(pm -> pm.fixedRate(1000)))
                .transform(File.class, fileStringGenericTransformer)
                .channel(producerChannel())
                .get();
    }


    @Bean
    IntegrationFlow ftp(DefaultFtpSessionFactory ftpSessionFactory) {
        return IntegrationFlows.from(this.producerChannel())
                .handle(Ftp.outboundAdapter(ftpSessionFactory)
                        .remoteDirectory("uploads")
                        .fileNameGenerator(message -> {
                            Object o = message.getHeaders().get(FileHeaders.FILENAME);
                            String fileName = String.class.cast(o);
                            return fileName.split("\\.")[0] + ".txt";
                        })
                )
                .get();
    }

    @Bean
    public DirectChannel producerChannel() {
        return new DirectChannel();
    }


    public static void main(String[] args) {
        SpringApplication.run(FilesIntegrationApplication.class, args);
    }
}
