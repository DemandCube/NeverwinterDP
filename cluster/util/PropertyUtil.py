class PropertyUtil(object):
  @staticmethod
  def traverseAndResolvePropertyExpression(obj, properties):
    if isinstance(obj, (int, long, float, complex, bool, basestring)): 
      return
    for name in obj.__dict__.keys():
      val = getattr(obj, name);
      if type(val) in (int, long, float, complex, bool): 
        continue
      elif isinstance(val, (basestring)):
        val = PropertyUtil.resolvePropertyExpression(val, properties)
        setattr(obj, name, val)
      elif isinstance(val, list):
        valList = val
        newList = [];
        for sel in valList:
          if isinstance(sel, (basestring)):
            sel = PropertyUtil.resolvePropertyExpression(sel, properties)
          else:
            PropertyUtil.traverseAndResolvePropertyExpression(sel, properties)
          newList.append(sel)
        del valList[:]
        valList.extend(newList)
      else: 
        PropertyUtil.traverseAndResolvePropertyExpression(val, properties)
  
  @staticmethod
  def resolvePropertyExpression(obj, properties):
    if not isinstance(obj, basestring): return obj
    for key in properties.__dict__.keys():
      if obj.find('${') >= 0:
        exp = '${' + key + '}'
        obj = obj.replace(exp, getattr(properties, key))
      else:
        break
    return obj
  