"""Evaluate an optionally-compressed FASTQ file for basic syntax."""


def has_valid_name(filename: str) -> bool:
    return filename.endswith('.fastq.gz') or filename.endswith('.fastq')
