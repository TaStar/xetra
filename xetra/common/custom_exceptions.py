"""Custom Exceptions"""

class WrongFormatException(Exception):
 """
  WrongFormatException class

  Exception that can be arised when the format type given as a parameter is not supported.
 """

class WrongMetaFileException(Exception):
  """
  WrongMetaFileException class

  Exception that can be arised when the meta file format is incorrect.
  """    