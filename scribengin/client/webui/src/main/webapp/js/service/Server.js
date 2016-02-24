define([
  'jquery'
], function($) {
  /**@type service.Server */
  Server = {
    /**@memberOf service.Server */
    syncGETResource : function(path, dataType) {
      var returnData = null ;
      $.ajax({ 
        type: "GET",
        dataType: dataType,
        url:  path,
        async: false ,
        error: function(data) {
          console.log(data) ;
          console.trace() ;
        },
        success: function(data) {  returnData = data ; }
      });
      return returnData ;
    },

    /**@memberOf service.Server */
    syncPOSTJson : function(path, dataObj) {
      var returnData = null ;
      $.ajax({ 
        async: false ,
        type: "POST",
        dataType: "json",
        contentType: "application/json; charset=utf-8",
        url: path,
        data:  JSON.stringify(dataObj) ,
        error: function(data) {  
          console.log("Error:") ; 
          console.printJSON(data) ; 
        },
        success: function(data) {  
          returnData = data ; 
        }
      });
      return returnData ;
    },
    
    /**@memberOf service.Server */
    restGET : function(restPath, params) {
      var restPathWithParams = restPath;
      var firstParam = true;
      for (prop in params) {
        if (!params.hasOwnProperty(prop)) {
          //The current property is not a direct property of params
          continue;
        }
        if(firstParam) {
          restPathWithParams += '?' + prop + '=' + params[prop];
          firstParam = false;
        } else {
          restPathWithParams += '&amp;' + prop + '=' + params[prop];
        }
      }

      var returnData = null ;
      $.ajax({ 
        type: "GET",
        dataType: "json",
        url: restPath,
        data: params ,
        async: false ,
        error: function(data) {  
          console.log("Error:") ; 
          console.log(data) ; 
        },
        success: function(data) {  returnData = data ; }
      });
      return returnData ;
    },
    
    /**@memberOf service.Server */
    restPOST : function(path, params) {
      var returnData = null ;
      $.ajax({ 
        async: false ,
        type: "POST",
        dataType: "json",
        contentType: "application/json; charset=utf-8",
        url: path,
        data:  JSON.stringify(params) ,
        error: function(data) {  
          console.debug("Error: \n" + JSON.stringify(data)) ; 
        },
        success: function(data) {  
          returnData = data ; 
        }
      });
      return returnData ;
    },

    /**@memberOf service.Server */
    cmdRestGET : function(restPath, params) {
      var cmdResponse = this.restGET(restPath, params) ;
      if(cmdResponse.error != null) {
        console.log("Request Error:") ;
        console.log("  Rest Path = " + restPath) ;
        console.log("  params = ") ;
        console.printJSON(params) ;
        console.log("  response = ") ;
        console.printJSON(cmdResponse) ;
        throw new Error('Error for the rest request ' + restPath);
      }
      return JSON.parse(cmdResponse.result);
    },

    /**@memberOf service.Server */
    cmdRestPOST : function(restPath, params) {
      var response = this.POST(restPath, params) ;
      if(response.data == null) {
        console.log("Request Error") ;
        console.printJSON(response) ;
      }
      return response.data ;
    }
  };
  return Server ;
});
