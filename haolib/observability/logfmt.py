"""Logfmt formatter."""

import logging


class LogfmtFormatter(logging.Formatter):
    """Formatter for logfmt.

    This formatter is used to format the log messages in logfmt format.
    It is used to format the log messages in logfmt format.
    """

    RESET = "\033[0m"
    """Reset color."""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[41m\033[97m",  # White on Red background
    }
    """Colors for the log levels."""

    def format(self, record: logging.LogRecord) -> str:
        """Format the log message in logfmt format."""

        level_color = self.COLORS.get(record.levelname, "")
        time_color = "\033[90m"  # Bright black (gray)
        logger_color = "\033[35m"  # Magenta
        caller_color = "\033[94m"  # Bright blue
        trace_color = "\033[95m"  # Bright magenta
        msg_color = "\033[0m"  # Default

        logfmt = [
            f'{time_color}time="{self.formatTime(record, self.datefmt)}"{self.RESET}',
            f"{level_color}level={record.levelname}{self.RESET}",
            f'{logger_color}logger="{record.name}"{self.RESET}',
            f'{caller_color}caller="{record.pathname}:{record.lineno}"{self.RESET}',
        ]

        trace_id = record.__dict__.get("otelTraceID")
        if trace_id:
            logfmt.append(f"{trace_color}trace_id={trace_id}{self.RESET}")

        span_id = record.__dict__.get("otelSpanID")
        if span_id:
            logfmt.append(f"{trace_color}span_id={span_id}{self.RESET}")

        msg = record.getMessage().replace("\n", "\\n").replace('"', '\\"')
        logfmt.append(f'{msg_color}msg="{msg}"{self.RESET}')

        # Add exception information if available
        if record.exc_info:
            exc_color = "\033[91m"  # Bright red for exceptions
            exc_text = self.formatException(record.exc_info)
            # Format exception text for logfmt (escape quotes and newlines)
            exc_formatted = exc_text  # exc_text.replace("\n", "\\n").replace('"', '\\"')
            logfmt.append(f'{exc_color}exception="{exc_formatted}"{self.RESET}')

        return " ".join(logfmt)
