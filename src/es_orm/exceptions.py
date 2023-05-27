class NotPerformedException(Exception):
	default_message = "The operation hasn't been performed successfully."

	def __init__(self, message: str = None, *args, **kwargs):
		if not message:
			message = self.default_message
		super().__init__(message, *args, **kwargs)


class NotConnectedException(NotPerformedException):
	default_message = "There was an error trying to establish the connection."


class InitializationRequired(NotPerformedException):
	default_message = "The initialization of index is required to continue."


class IOImportException(NotPerformedException):
	default_message = "There was an error importing the infoobject."


class IndexNotInitializedException(NotPerformedException):
	default_message = "The index is not initialized."


class ValidationException(NotPerformedException):
	default_message = "There was an error during validation."




