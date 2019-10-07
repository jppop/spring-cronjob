package me.sample.cronjob.config;

import me.sample.cronjob.domain.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.messaging.Message;

import javax.sql.DataSource;
import java.util.List;

@Configuration
@EnableIntegration
@EnableBatchProcessing
public class BatchConfiguration {

    private final static Logger logger = LoggerFactory.getLogger(BatchConfiguration.class);

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobRepository jobRepository;

    @Bean
    public DataSource dataSource() {
        EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
        EmbeddedDatabase db = builder
                .setType(EmbeddedDatabaseType.HSQL) //.H2 or .DERBY
                .addScripts("schema-all.sql", "data.sql")
                .build();
        return db;
    }

    @Bean
    public Job mailerJob() {
        return jobBuilderFactory.get("mailerJob")
                .incrementer(new RunIdIncrementer())
                .flow(mailStep())
                .end()
                .build();
    }

    @Bean
    Step mailStep() {
        return stepBuilderFactory.get("step1")
                .<Person, Person>chunk(2)
                .reader(mailerReader())
                .writer(mailerWriter())
                .build();
    }

    @Bean
    ItemReader<? extends Person> mailerReader() {
        JdbcCursorItemReader<Person> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource());
        reader.setSql("select first_name, last_name, email from person");
        reader.setRowMapper(new BeanPropertyRowMapper<>(Person.class));
        return reader;
    }

    @Bean
    ItemWriter<? super Person> mailerWriter() {
        return new ItemWriter<Person>() {
            @Override
            public void write(List<? extends Person> list) throws Exception {
                list.stream().forEach(item -> logger.info("Writing {}", item));
            }
        };
    }

    @Bean
    public JobLaunchingGateway jobLaunchingGateway() {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);
        simpleJobLauncher.setTaskExecutor(new SyncTaskExecutor());
        JobLaunchingGateway jobLaunchingGateway = new JobLaunchingGateway(simpleJobLauncher);

        return jobLaunchingGateway;
    }

    @Bean
    public DirectChannel mailerChannel() {
        return new DirectChannel();
    }

    @Bean
    MessageSource<String> mailerJobMessageSource() {
        return () -> MessageBuilder.withPayload("mailer").build();
    }

    @Bean
    public JobRequestTransformer mailerStartToJobRequest() {
        JobRequestTransformer jobRequestTransformer = new JobRequestTransformer();
        jobRequestTransformer.setJob(mailerJob());
        return jobRequestTransformer;
    }

    @Bean
    public IntegrationFlow integrationFlow(JobLaunchingGateway jobLaunchingGateway) {
        return IntegrationFlows
                .from(mailerJobMessageSource(), c -> c.poller(Pollers.fixedRate(5000, 2000)))
                .transform(mailerStartToJobRequest())
                .log(LoggingHandler.Level.INFO, "Starting")
                .handle(jobLaunchingGateway)
                .log(LoggingHandler.Level.INFO, "headers.id + ': ' + payload")
                .get();
    }

    /**
     * JobRequestTransformer creates a JobLaunchRequest with a timestamp parameters
     * (allowing to start a completed job).
     */
    public class JobRequestTransformer {
        private Job job;

        public Job job() {
            return job;
        }

        public void setJob(Job job) {
            this.job = job;
        }

        @Transformer
        public JobLaunchRequest toRequest(Message<String> message) {
            JobParametersBuilder jobParametersBuilder =
                    new JobParametersBuilder();

            jobParametersBuilder.addLong("time", System.currentTimeMillis());

            return new JobLaunchRequest(job, jobParametersBuilder.toJobParameters());
        }
    }}
