package common_datalayer

func NewLogger() Logger {
	return nil
}

type LogMessage interface {
	GetMessage() map[string]interface{}
}

type Logger interface {
	LogError(message LogMessage)
	LogInfo(message LogMessage)
	LogDebug(message LogMessage)
}
