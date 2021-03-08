var cc = DataStudioApp.createCommunityConnector();

function getAuthType() {
  var AuthTypes = cc.AuthType;
  return cc
    .newAuthTypeResponse()
    .setAuthType(AuthTypes.NONE)
    .build();
}

function isAdminUser() {
  return true;
}

// CONFIG --------------------------------------------------------------------
function getConfig(request) {
  var config = cc.getConfig();
  
  config.newInfo()
    .setId('instructions')
    .setText('Enter the id of a Socrata data set and the SoQL query parameters ');
  
  config.newTextInput()
    .setId('socrata_id')
    .setName('Enter the id for a Socrata data set')
    .setHelpText("e.g. yqn4-3th2")
    .setPlaceholder('yqn4-3th2');

    config.newTextInput()
    .setId('query_parameters')
    .setName('Enter the query using SoQL, the Socrata Query Language')
    .setHelpText("SELECT * WHERE violation_date > '2020-01-01T00:00:00' AND violation_date <'2021-01-01T00:00:00'")
    .setPlaceholder('Query');
  
  return config.build();
}


// SCHEMA --------------------------------------------------------------------

function getMetadata(socrata_id) {
  var discovery_api = "http://api.us.socrata.com/api/catalog/v1?ids=";
  var metadata_url = discovery_api + socrata_id;
  var response = UrlFetchApp.fetch(metadata_url);
  var metadata = JSON.parse(response.getContentText());
  return metadata.results[0];
}


function getFields(metadata) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;
  
  var typeMap = {
    "text": types.TEXT,
    "number": types.NUMBER,
    "calendar_date": types.TEXT,
    "point": types.LATITUDE_LONGITUDE
  };

  for (var f = 0; f< metadata.resource.columns_field_name.length; f++) {
    var socrataType = metadata.resource.columns_datatype[f];
    fields.newDimension()
      .setId(metadata.resource.columns_field_name[f])
      .setName(metadata.resource.columns_name[f])
      .setType(typeMap[socrataType])
      .setDescription(metadata.resource.columns_description[f]);
    
    if (socrataType == "calendar_date") { 
      var formula = 'PARSE_DATE("%Y-%m-%d", LEFT_TEXT($' + metadata.resource.columns_field_name[f] + ', 10))';
      
      fields.newDimension()
        .setId(metadata.resource.columns_field_name[f] + '_DO')
        .setName(metadata.resource.columns_name[f] + '_DO')
        .setType(types.YEAR_MONTH_DAY)
        .setDescription('Date Object Converted From String ' + metadata.resource.columns_field_name[f])
        .setFormula(formula);

    } 
  }
  
  fields.newDimension()
    .setId('json_index')
    .setName('Index')
    .setType(types.NUMBER)
    .setDescription('Not part of source data - index of record in JSON response object');
  
  fields.newMetric()
    .setId('record_count')
    .setName('Record Count')
    .setType(types.NUMBER)
    .setFormula('COUNT($json_index)')
    .setAggregation(aggregations.AUTO);

  return fields;
}


function getSchema(request) {
  var metadata = getMetadata(request.configParams.socrata_id);
  var fields = getFields(metadata).build();
  return { schema: fields };
}



// DATA --------------------------------------------------------------------

function responseToRows(requestedFields, response) {
  var cc = DataStudioApp.createCommunityConnector();
  var types = cc.FieldType;
  var rows = [];

  for (var r = 0; r < response.length; r++) { 
    var source_row = response[r];
    var selected_values = [];
    requestedFields.asArray().forEach(function(field) {
      var column = field.getId();
//      console.log(column, field.getType() );
      if (column == "json_index") {
        return selected_values.push(r);
      } else {
        return selected_values.push(source_row[column]);
      }
    });
//    console.log(selected_values);
    rows.push( {values: selected_values} );
  }
  return rows;  
}

function getData(request) {
  
  try {
    var metadata = getMetadata(request.configParams.socrata_id);
    var requestedFieldIds = request.fields.map(function(field) {
      return field.name;
    });
//    requestedFieldIds.push('json_index')
    console.log('Requested field IDs: ', requestedFieldIds);
    var requestedFields = getFields(metadata).forIds(requestedFieldIds);
    
    // Fetch and parse data from API
    var data_url = 'https://' + metadata.metadata.domain + '/resource/'+ request.configParams.socrata_id + '.json?$query=' + request.configParams.query_parameters;
    
    console.log(data_url);
    
    var response = UrlFetchApp.fetch(encodeURI(data_url));
    var parsedResponse = JSON.parse(response.getContentText());
    var rows = responseToRows(requestedFields, parsedResponse);
  }
  
  catch(e) {
    var cc = DataStudioApp.createCommunityConnector()
      .newUserError()
      .setDebugText('Error fetching data from API. Exception details: ' + e)
      .setText('There was an error communicating with the service. Try again later, or file an issue if this error persists.')
      .throwException();
  }

  return {
    schema: requestedFields.build(),
    rows: rows
  };

}



