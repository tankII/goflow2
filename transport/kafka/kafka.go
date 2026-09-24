// Package kafka implements a Kafka transport using sarama.
package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/netsampler/goflow2/v3/transport"

	sarama "github.com/Shopify/sarama"
)

// KafkaDriver sends formatted messages to Kafka topics.
type KafkaDriver struct {
	kafkaTLS         bool
	kafkaClientCert  string
	kafkaClientKey   string
	kafkaServerCA    string
	kafkaTlsInsecure bool

	kafkaSASL           string
	kafkaTopic          string
	kafkaSrv            string
	kafkaBrk            string
	kafkaMaxMsgBytes    int
	kafkaFlushBytes     int
	kafkaFlushFrequency time.Duration

	kafkaHashing          bool
	kafkaVersion          string
	kafkaCompressionCodec string

	kafkaSASLKerberosKeytab          string
	kafkaSASLKerberosUsername        string
	kafkaSASLKerberosRealm           string
	kafkaSASLKerberosServiceName     string
	kafkaSASLKerberosConfig          string
	kafkaSASLKerberosDisablePAFXFAST bool

	producer sarama.AsyncProducer

	errors chan error
}

// KafkaTransportError wraps errors returned by the Kafka client.
type KafkaTransportError struct {
	Err error
}

func (e *KafkaTransportError) Error() string {
	return fmt.Sprintf("kafka transport %s", e.Err.Error())
}
func (e *KafkaTransportError) Unwrap() []error {
	return []error{transport.ErrTransport, e.Err}
}

// KafkaSASLAlgorithm names supported SASL auth mechanisms.
type KafkaSASLAlgorithm string

const (
	KAFKA_SASL_NONE         KafkaSASLAlgorithm = "none"
	KAFKA_SASL_PLAIN        KafkaSASLAlgorithm = "plain"
	KAFKA_SASL_SCRAM_SHA256 KafkaSASLAlgorithm = "scram-sha256"
	KAFKA_SASL_SCRAM_SHA512 KafkaSASLAlgorithm = "scram-sha512"
	KAFKA_SASL_GSSAPI       KafkaSASLAlgorithm = "gssapi"
)

var (
	compressionCodecs = map[string]sarama.CompressionCodec{
		strings.ToLower(sarama.CompressionNone.String()):   sarama.CompressionNone,
		strings.ToLower(sarama.CompressionGZIP.String()):   sarama.CompressionGZIP,
		strings.ToLower(sarama.CompressionSnappy.String()): sarama.CompressionSnappy,
		strings.ToLower(sarama.CompressionLZ4.String()):    sarama.CompressionLZ4,
		strings.ToLower(sarama.CompressionZSTD.String()):   sarama.CompressionZSTD,
	}

	saslAlgorithms = map[KafkaSASLAlgorithm]bool{
		KAFKA_SASL_PLAIN:        true,
		KAFKA_SASL_SCRAM_SHA256: true,
		KAFKA_SASL_SCRAM_SHA512: true,
		KAFKA_SASL_GSSAPI:       true,
	}
	saslAlgorithmsList = []string{
		string(KAFKA_SASL_NONE),
		string(KAFKA_SASL_PLAIN),
		string(KAFKA_SASL_SCRAM_SHA256),
		string(KAFKA_SASL_SCRAM_SHA512),
		string(KAFKA_SASL_GSSAPI),
	}
)

func (d *KafkaDriver) Prepare() error {
	flag.BoolVar(&d.kafkaTLS, "transport.kafka.tls", false, "Use TLS to connect to Kafka")

	flag.StringVar(&d.kafkaClientCert, "transport.kafka.tls.client", "", "Kafka client certificate")
	flag.StringVar(&d.kafkaClientKey, "transport.kafka.tls.key", "", "Kafka client key")
	flag.StringVar(&d.kafkaServerCA, "transport.kafka.tls.ca", "", "Kafka certificate authority")
	flag.BoolVar(&d.kafkaTlsInsecure, "transport.kafka.tls.insecure", false, "Skips TLS verification")

	flag.StringVar(&d.kafkaSASL, "transport.kafka.sasl", "none",
		fmt.Sprintf(
			"Use SASL to connect to Kafka, available settings: %s (for plain/scram, TLS is recommended and the environment variables KAFKA_SASL_USER and KAFKA_SASL_PASS need to be set)",
			strings.Join(saslAlgorithmsList, ", ")))

	flag.StringVar(&d.kafkaTopic, "transport.kafka.topic", "flow-messages", "Kafka topic to produce to")
	flag.StringVar(&d.kafkaSrv, "transport.kafka.srv", "", "SRV record containing a list of Kafka brokers (or use brokers)")
	flag.StringVar(&d.kafkaBrk, "transport.kafka.brokers", "127.0.0.1:9092,[::1]:9092", "Kafka brokers list separated by commas")
	flag.IntVar(&d.kafkaMaxMsgBytes, "transport.kafka.maxmsgbytes", 1000000, "Kafka max message bytes")
	flag.IntVar(&d.kafkaFlushBytes, "transport.kafka.flushbytes", int(sarama.MaxRequestSize), "Kafka flush bytes")
	flag.DurationVar(&d.kafkaFlushFrequency, "transport.kafka.flushfreq", time.Second*5, "Kafka flush frequency")

	flag.BoolVar(&d.kafkaHashing, "transport.kafka.hashing", false, "Enable partition hashing")

	flag.StringVar(&d.kafkaVersion, "transport.kafka.version", "2.8.0", "Kafka version")
	flag.StringVar(&d.kafkaCompressionCodec, "transport.kafka.compression", "", "Kafka default compression")

	flag.StringVar(&d.kafkaSASLKerberosKeytab, "transport.kafka.sasl.kerberos.keytab", "", "Kafka SASL (gssapi) keytab file path")
	flag.StringVar(&d.kafkaSASLKerberosUsername, "transport.kafka.sasl.kerberos.username", "", "Kafka SASL (gssapi) client principal (without realm)")
	flag.StringVar(&d.kafkaSASLKerberosRealm, "transport.kafka.sasl.kerberos.realm", "", "Kafka SASL (gssapi) client realm")
	flag.StringVar(&d.kafkaSASLKerberosServiceName, "transport.kafka.sasl.kerberos.service-name", "kafka", "Kafka SASL (gssapi) service name")
	flag.StringVar(&d.kafkaSASLKerberosConfig, "transport.kafka.sasl.kerberos.config", "", "Path to krb5.conf (defaults to KRB5_CONFIG or /etc/krb5.conf)")
	flag.BoolVar(&d.kafkaSASLKerberosDisablePAFXFAST, "transport.kafka.sasl.kerberos.disable-paf-x-fast", false, "Disable PA-FX-FAST when using SASL (gssapi)")

	return nil
}

// Errors returns the Kafka producer error channel.
func (d *KafkaDriver) Errors() <-chan error {
	return d.errors
}

// buildKafkaConfig assembles the sarama configuration from the driver flags.
func (d *KafkaDriver) buildKafkaConfig() (*sarama.Config, error) {
	kafkaConfigVersion, err := sarama.ParseKafkaVersion(d.kafkaVersion)
	if err != nil {
		return nil, err
	}

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = kafkaConfigVersion
	kafkaConfig.Producer.Return.Successes = false
	kafkaConfig.Producer.Return.Errors = true
	kafkaConfig.Producer.MaxMessageBytes = d.kafkaMaxMsgBytes
	kafkaConfig.Producer.Flush.Bytes = d.kafkaFlushBytes
	kafkaConfig.Producer.Flush.Frequency = d.kafkaFlushFrequency
	kafkaConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	if d.kafkaCompressionCodec != "" {
		if cc, ok := compressionCodecs[strings.ToLower(d.kafkaCompressionCodec)]; !ok {
			return nil, fmt.Errorf("compression codec does not exist")
		} else {
			kafkaConfig.Producer.Compression = cc
		}
	}

	if d.kafkaTLS {
		rootCAs, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("error initializing TLS: %v", err)
		}
		kafkaConfig.Net.TLS.Enable = true
		kafkaConfig.Net.TLS.Config = &tls.Config{
			RootCAs:    rootCAs,
			MinVersion: tls.VersionTLS12,
		}

		kafkaConfig.Net.TLS.Config.InsecureSkipVerify = d.kafkaTlsInsecure

		if d.kafkaServerCA != "" {
			serverCaFile, err := os.Open(d.kafkaServerCA)
			if err != nil {
				return nil, fmt.Errorf("error initializing server CA: %v", err)
			}

			serverCaBytes, err := io.ReadAll(serverCaFile)
			if err != nil {
				if closeErr := serverCaFile.Close(); closeErr != nil {
					return nil, fmt.Errorf("error closing server CA: %v", closeErr)
				}
				return nil, fmt.Errorf("error reading server CA: %v", err)
			}
			if err := serverCaFile.Close(); err != nil {
				return nil, fmt.Errorf("error closing server CA: %v", err)
			}

			block, _ := pem.Decode(serverCaBytes)

			serverCa, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("error parsing server CA: %v", err)
			}

			certPool := x509.NewCertPool()
			certPool.AddCert(serverCa)

			kafkaConfig.Net.TLS.Config.RootCAs = certPool
		}

		if d.kafkaClientCert != "" && d.kafkaClientKey != "" {
			_, err := tls.LoadX509KeyPair(d.kafkaClientCert, d.kafkaClientKey)
			if err != nil {
				return nil, fmt.Errorf("error initializing mTLS: %v", err)
			}

			kafkaConfig.Net.TLS.Config.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				cert, err := tls.LoadX509KeyPair(d.kafkaClientCert, d.kafkaClientKey)
				if err != nil {
					return nil, err
				}
				return &cert, nil
			}
		}

	}

	if d.kafkaHashing {
		kafkaConfig.Producer.Partitioner = sarama.NewHashPartitioner
	}

	kafkaSASL := KafkaSASLAlgorithm(d.kafkaSASL)
	if d.kafkaSASL != "" && kafkaSASL != KAFKA_SASL_NONE {
		_, ok := saslAlgorithms[KafkaSASLAlgorithm(strings.ToLower(d.kafkaSASL))]
		if !ok {
			return nil, errors.New("sasl algorithm does not exist")
		}

		kafkaConfig.Net.SASL.Enable = true

		if kafkaSASL == KAFKA_SASL_GSSAPI {
			if d.kafkaSASLKerberosKeytab == "" {
				return nil, fmt.Errorf("kafka SASL kerberos config was unsuccessful: transport.kafka.sasl.kerberos.keytab needs to be set")
			}
			if d.kafkaSASLKerberosUsername == "" {
				return nil, fmt.Errorf("kafka SASL kerberos config was unsuccessful: transport.kafka.sasl.kerberos.username needs to be set")
			}
			if d.kafkaSASLKerberosRealm == "" {
				return nil, fmt.Errorf("kafka SASL kerberos config was unsuccessful: transport.kafka.sasl.kerberos.realm needs to be set")
			}

			kcfg := d.kafkaSASLKerberosConfig
			switch {
			case strings.TrimSpace(kcfg) != "":
			case os.Getenv("KRB5_CONFIG") != "":
				kcfg = os.Getenv("KRB5_CONFIG")
			default:
				if _, err := os.Stat("/etc/krb5.conf"); err == nil {
					kcfg = "/etc/krb5.conf"
				}
			}
			if kcfg == "" {
				return nil, fmt.Errorf("kafka SASL kerberos config was unsuccessful: transport.kafka.sasl.kerberos.config must be set, or KRB5_CONFIG environment variable or /etc/krb5.conf must exist")
			}

			kafkaConfig.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
			kafkaConfig.Net.SASL.GSSAPI = sarama.GSSAPIConfig{
				AuthType:           sarama.KRB5_KEYTAB_AUTH,
				Username:           d.kafkaSASLKerberosUsername,
				KeyTabPath:         d.kafkaSASLKerberosKeytab,
				KerberosConfigPath: kcfg,
				ServiceName:        d.kafkaSASLKerberosServiceName,
				Realm:              d.kafkaSASLKerberosRealm,
				DisablePAFXFAST:    d.kafkaSASLKerberosDisablePAFXFAST,
			}
		} else {
			kafkaConfig.Net.SASL.User = os.Getenv("KAFKA_SASL_USER")
			kafkaConfig.Net.SASL.Password = os.Getenv("KAFKA_SASL_PASS")
			if kafkaConfig.Net.SASL.User == "" && kafkaConfig.Net.SASL.Password == "" {
				return nil, fmt.Errorf("kafka SASL config from environment was unsuccessful: KAFKA_SASL_USER and KAFKA_SASL_PASS need to be set")
			}

			if kafkaSASL == KAFKA_SASL_SCRAM_SHA256 || kafkaSASL == KAFKA_SASL_SCRAM_SHA512 {
				kafkaConfig.Net.SASL.Handshake = true

				switch kafkaSASL {
				case KAFKA_SASL_SCRAM_SHA512:
					kafkaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
						return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
					}
					kafkaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
				case KAFKA_SASL_SCRAM_SHA256:
					kafkaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
						return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
					}
					kafkaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
				}
			}
		}
	}

	return kafkaConfig, nil
}

// Init configures the Kafka producer and establishes connections.
func (d *KafkaDriver) Init() error {
	kafkaConfig, err := d.buildKafkaConfig()
	if err != nil {
		return err
	}

	var addrs []string
	if d.kafkaSrv != "" {
		addrs, _ = GetServiceAddresses(d.kafkaSrv)
	} else {
		addrs = strings.Split(d.kafkaBrk, ",")
	}

	kafkaProducer, err := sarama.NewAsyncProducer(addrs, kafkaConfig)
	if err != nil {
		return err
	}
	d.producer = kafkaProducer

	go func() {
		for msg := range kafkaProducer.Errors() {
			select {
			case d.errors <- &KafkaTransportError{msg}:
			default:
			}
		}
		close(d.errors)
	}()

	return err
}

// Send publishes a message to Kafka.
func (d *KafkaDriver) Send(key, data []byte) error {
	d.producer.Input() <- &sarama.ProducerMessage{
		Topic: d.kafkaTopic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(data),
	}
	return nil
}

// Close stops the producer and releases resources.
func (d *KafkaDriver) Close() error {
	return d.producer.Close()
}

// GetServiceAddresses resolves SRV records into broker addresses.
func GetServiceAddresses(srv string) (addrs []string, err error) {
	_, srvs, err := net.LookupSRV("", "", srv)
	if err != nil {
		return nil, fmt.Errorf("service discovery: %v", err)
	}
	for _, srv := range srvs {
		addrs = append(addrs, net.JoinHostPort(srv.Target, strconv.Itoa(int(srv.Port))))
	}
	return addrs, nil
}

func init() {
	d := &KafkaDriver{
		errors: make(chan error),
	}
	transport.RegisterTransportDriver("kafka", d)
}
