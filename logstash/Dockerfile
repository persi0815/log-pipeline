FROM docker.elastic.co/logstash/logstash:9.3.0

# Logstash 9.3.0 기본 이미지에 Kafka integration plugin 12.1.9를 설치한 커스텀 Logstash 이미지를 만든다”
RUN bin/logstash-plugin install --version 12.1.9 logstash-integration-kafka
