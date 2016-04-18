import os, shutil

class FileUtil(object):
  @staticmethod
  def updateWithProperties(src, dst, properties):
    srcFile = open(src, 'r')
    srcContent = srcFile.read();
    srcFile.close()
    for key in properties:
      exp = '${' + key + '}'
      srcContent = srcContent.replace(exp, properties[key])
    destinationFile = open(dst, 'w')
    destinationFile.write(srcContent)
    destinationFile.close()
  
  @staticmethod
  def readAsText(path):
    dstFile = open(path, 'r')
    content = dstFile.read();
    dstFile.close()
    return content
  
  @staticmethod
  def writeText(path, text):
    dstFile = open(path, 'w')
    dstFile.write(text)
    dstFile.close()
    
  @staticmethod
  def rmtree(path): shutil.rmtree(path)

  @staticmethod
  def rm(path): os.remove(path)
  
  @staticmethod
  def mv(source, dest): shutil.move(source, dest)
  
  @staticmethod
  def mkdirs(path): os.makedirs(path)
  
  @staticmethod
  def cptree(src, dest): shutil.copytree(src, dest)
  