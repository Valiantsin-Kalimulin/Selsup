/*
 * (c) 2024 Valiantsin Kalimulin. All Right Reserved. All information contained herein is, and remains the
 * property of Valiantsin Kalimulin and/or its suppliers and is protected by international intellectual
 * property law. Dissemination of this information or reproduction of this material is strictly forbidden,
 * unless prior written permission is obtained from Valiantsin Kalimulin
 */

package com.test.crpt;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Optional;
import java.util.concurrent.Semaphore;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.EntityNotFoundException;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import static com.test.crpt.CrptApi.ExceptionCode.DOCUMENT_UPLOAD_ERROR;
import static java.time.temporal.ChronoUnit.SECONDS;

@Slf4j
@Service
@RequiredArgsConstructor
public class CrptApi {

    private static final ThreadLocal<LocalTime> DOCUMENT_SENDING_START_AT = new ThreadLocal<>();
    private static final ThreadLocal<LocalTime> DOCUMENT_SENDING_FINISHED_AT = new ThreadLocal<>();
    // exceeding the limit on the number of requests is prohibited, just a safety for avoid excess
    private static final int ADDITIONAL_DELAY_SECONDS = 1;

    @Value("${crpt.settings.interval_seconds:5}")
    private final long timeAccessInterval;

    @Value("${crpt.settings.requestLimit:10}")
    private final int requestLimits;

    private final Semaphore semaphore = new Semaphore(requestLimits, true);
    private final DocumentService documentService;

    public void sendDocument(Document document) {
        try {
            semaphore.acquire();
            DOCUMENT_SENDING_START_AT.set(LocalTime.now());
            documentService.sendDocumentToTrueSign(document);
            DOCUMENT_SENDING_FINISHED_AT.set(LocalTime.now());
            controlRequestDuration();
            semaphore.release();
        } catch (InterruptedException e) {
            log.error("The document was not sent: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void controlRequestDuration() throws InterruptedException {
        var  delta = SECONDS.between(DOCUMENT_SENDING_START_AT.get(), DOCUMENT_SENDING_FINISHED_AT.get());
        var durationLimit = timeAccessInterval + ADDITIONAL_DELAY_SECONDS;
        if (delta < durationLimit) {
            Thread.sleep(durationLimit - delta);
        }
    }

    @Service
    @Slf4j
    @RequiredArgsConstructor
    public class DocumentSendingServiceImpl implements DocumentSendingService {

        @Value("${crpt.urls.true_sign:https://ismp.crpt.ru/api/v3/lk/documents/create}")
        private String trueSignUrl;

        @Override
        public void sendDocument(Document document, String urlValue) {
            log.debug("The document begins to be sent");
            try {
                final Response postResult = Request.Post(urlValue)
                        .bodyString(new ObjectMapper().writeValueAsString(document), ContentType.APPLICATION_JSON)
                        .execute();
                final var status = HttpStatus.resolve(postResult.returnResponse().getStatusLine().getStatusCode());
                ResponseUtils.checkResponseStatus(status);
            } catch (IOException e) {
                log.error("The document was not sent: {}", e.getMessage());
                throw new RuntimeException(e);
            }
            log.debug("The document has been sent successfully");
        }

        @Override
        public String getTrueSingUrl() {
            return trueSignUrl;
        }
    }

    @Service
    @Slf4j
    @RequiredArgsConstructor
    public class DocumentServiceImpl implements DocumentService {

        private final DocumentRepository documentRepository;
        private final DocumentSendingService documentSendingService;

        @Override
        @Transactional(readOnly = true)
        public Optional<Document> findById(Long id) {
            return documentRepository.findById(id);
        }

        @Override
        @Transactional
        public void sendDocumentToTrueSign(Document document) {
            documentRepository.findById(document.getId()).orElseThrow(EntityNotFoundException::new);
            var url = documentSendingService.getTrueSingUrl();
            documentSendingService.sendDocument(document, url);
        }
    }

    public interface DocumentSendingService {
        void sendDocument(Document document, String urlValue);
        String getTrueSingUrl();
    }

    public interface DocumentService {
        Optional<Document> findById(Long id);
        void sendDocumentToTrueSign(Document document);
    }

    public interface DocumentRepository extends JpaRepository<Document, Long> {
    }

    @Data
    @Entity
    @Table(schema = "crpt_sys", name = "documents")
    public class Document implements Serializable {
        // There is no data in the task where and how to use the signature
        @Serial
        private static final long serialVersionUID = 1L;

        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        @Column(name = "document_id")
        private Long id;

        private String description;
        private String participantInn;
        private Long docId;
        private String docStatus;
        private String docType;
        private boolean importRequest;
        private String ownerInn;
        private String participant_Inn;
        private String producerInn;
        private LocalDate productionDate;
        private String productionType;

        @Embedded
        private Products products;

        private LocalDate regDate;
        private String regNumber;
    }

    @Data
    @Embeddable
    public class Products implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private String certificateDocument;
        private LocalDate certificateDocumentDate;
        private String certificateDocumentNumber;
        private String ownerInn;
        private String producerInn;
        private LocalDate productionDate;
        private String tnvedCode;
        private String uitCode;
        private String uituCode;
    }

    @UtilityClass
    public class ResponseUtils {
        public void checkResponseStatus(HttpStatus status) {
            if (status == null || !status.is2xxSuccessful()) {
                log.error("The document was not sent, external server error: {}", DOCUMENT_UPLOAD_ERROR);
                throw new RuntimeException(DOCUMENT_UPLOAD_ERROR);
            }
        }
    }

    public static final class ExceptionCode {
        public static final String DOCUMENT_UPLOAD_ERROR = "document.upload.error";
    }
}
