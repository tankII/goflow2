package kafka

import (
	"testing"

	sarama "github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

func newKafkaTestDriver() *KafkaDriver {
	return &KafkaDriver{
		kafkaVersion:     "2.8.0",
		kafkaBrk:         "127.0.0.1:9092",
		kafkaTopic:       "flow-messages",
		kafkaMaxMsgBytes: 1000000,
		kafkaFlushBytes:  int(sarama.MaxRequestSize),
		errors:           make(chan error),
	}
}

func TestBuildKafkaConfigGSSAPI(t *testing.T) {
	d := newKafkaTestDriver()
	d.kafkaSASL = "gssapi"
	d.kafkaSASLKerberosUsername = "goflow2"
	d.kafkaSASLKerberosKeytab = "/etc/krb5/goflow2.keytab"
	d.kafkaSASLKerberosRealm = "EXAMPLE.COM"
	d.kafkaSASLKerberosServiceName = "kafka"
	d.kafkaSASLKerberosConfig = "/etc/krb5.conf"
	d.kafkaSASLKerberosDisablePAFXFAST = true

	cfg, err := d.buildKafkaConfig()
	assert.Nil(t, err)
	assert.True(t, cfg.Net.SASL.Enable)
	assert.Equal(t, string(sarama.SASLTypeGSSAPI), string(cfg.Net.SASL.Mechanism))
	assert.Equal(t, sarama.KRB5_KEYTAB_AUTH, cfg.Net.SASL.GSSAPI.AuthType)
	assert.Equal(t, "goflow2", cfg.Net.SASL.GSSAPI.Username)
	assert.Equal(t, "/etc/krb5/goflow2.keytab", cfg.Net.SASL.GSSAPI.KeyTabPath)
	assert.Equal(t, "EXAMPLE.COM", cfg.Net.SASL.GSSAPI.Realm)
	assert.Equal(t, "kafka", cfg.Net.SASL.GSSAPI.ServiceName)
	assert.Equal(t, "/etc/krb5.conf", cfg.Net.SASL.GSSAPI.KerberosConfigPath)
	assert.True(t, cfg.Net.SASL.GSSAPI.DisablePAFXFAST)
	assert.Nil(t, cfg.Validate())
}

func TestBuildKafkaConfigGSSAPINoKeytab(t *testing.T) {
	d := newKafkaTestDriver()
	d.kafkaSASL = "gssapi"

	_, err := d.buildKafkaConfig()
	assert.NotNil(t, err)
}

func TestBuildKafkaConfigPlainNoEnv(t *testing.T) {
	t.Setenv("KAFKA_SASL_USER", "")
	t.Setenv("KAFKA_SASL_PASS", "")

	d := newKafkaTestDriver()
	d.kafkaSASL = "plain"

	_, err := d.buildKafkaConfig()
	assert.NotNil(t, err)
}

func TestBuildKafkaConfigPlainWithEnv(t *testing.T) {
	t.Setenv("KAFKA_SASL_USER", "collector")
	t.Setenv("KAFKA_SASL_PASS", "secret")

	d := newKafkaTestDriver()
	d.kafkaSASL = "plain"

	cfg, err := d.buildKafkaConfig()
	assert.Nil(t, err)
	assert.Equal(t, "collector", cfg.Net.SASL.User)
	assert.Equal(t, "secret", cfg.Net.SASL.Password)
}

func TestBuildKafkaConfigSCRAM512(t *testing.T) {
	t.Setenv("KAFKA_SASL_USER", "collector")
	t.Setenv("KAFKA_SASL_PASS", "secret")

	d := newKafkaTestDriver()
	d.kafkaSASL = "scram-sha512"

	cfg, err := d.buildKafkaConfig()
	assert.Nil(t, err)
	assert.True(t, cfg.Net.SASL.Handshake)
	assert.Equal(t, string(sarama.SASLTypeSCRAMSHA512), string(cfg.Net.SASL.Mechanism))
}

func TestBuildKafkaConfigGSSAPINoKrb5Conf(t *testing.T) {
	// Temporarily remove KRB5_CONFIG and /etc/krb5.conf to test error path
	t.Setenv("KRB5_CONFIG", "")

	// This test assumes /etc/krb5.conf does not exist on this machine
	// which is true in this environment
	d := newKafkaTestDriver()
	d.kafkaSASL = "gssapi"
	d.kafkaSASLKerberosUsername = "goflow2"
	d.kafkaSASLKerberosKeytab = "/etc/krb5/goflow2.keytab"
	d.kafkaSASLKerberosRealm = "EXAMPLE.COM"
	// Do NOT set config flag

	_, err := d.buildKafkaConfig()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "kerberos config")
}
