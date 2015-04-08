/*
 * JsonSQL Extension
 * By: Mario Muellner
 * Version 0.2
 * Last Modified: 30/03/2015
 * 
 * Add Support for Whitespaces in Fieldlist
 * Add Parsing for Group By and Aggregat Functions
 * Add Group by Functionality
 * Add SUpport for 0 based Limits
 *  
 */
 
/*
 * JsonSQL
 * By: Trent Richardson [http://trentrichardson.com]
 * Version 0.1
 * Last Modified: 1/1/2008
 * 
 * Copyright 2008 Trent Richardson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var jsonsql = {
		
	query: function(sql,json){

		//var returnfields = sql.match(/^(select)\s+([a-z0-9_\,\.\s\*]+)\s+from\s+([a-z0-9_\.]+)(?: where\s+\((.+)\))?\s*(?:order\sby\s+([a-z0-9_\,]+))?\s*(ascnum|descnum|asc|desc)?\s*(?:limit\s+([0-9_\,]+))?/i);
		//var returnfields = sql.match(/^(select)\s+([a-z0-9_\,\.\s\*]+)\s+from\s+([a-z0-9_\.]+)(?: where\s+\((.+)\))?\s*(?:group\sby\s+([a-z0-9_\,]+))?\s*(?:order\sby\s+([a-z0-9_\,]+))?\s*(asc|desc|ascnum|descnum)?\s*(?:limit\s+([0-9_\,]+))?/i);
		var returnfields = sql.match(/^(select)\s+([a-z0-9_\,\.\s\*()]+)\s+from\s+([a-z0-9_\.]+)(?: where\s+\((.+)\))?\s*(?:group\sby\s+([a-z0-9_\,]+))?\s*(?:order\sby\s+([a-z0-9_\,]+))?\s*(asc|desc|ascnum|descnum)?\s*(?:limit\s+([0-9_\,]+))?/i);
		
		var fields = this.arrayTrim(returnfields[2].replace(' ','').split(','));
		var aggregat = this.getAggregatArray(fields);		
		fields = this.getAggregatFreeArray(fields);		
		var ops = { 
			fields: fields, 
			from: returnfields[3].replace(' ',''), 
			where: (returnfields[4] == undefined)? "true":returnfields[4],
			groupby: (returnfields[5] == undefined)? []:returnfields[5].replace(' ','').split(','),
			aggregat: aggregat,
			orderby: (returnfields[6] == undefined)? []:returnfields[6].replace(' ','').split(','),
			order: (returnfields[7] == undefined)? "asc":returnfields[7],
			limit: (returnfields[8] == undefined)? []:returnfields[8].replace(' ','').split(',')
		};		
		console.log(ops);
		//console.log(json);
		return this.parse(json, ops);		
	},
	
	parse: function(json,ops){
		var o = { fields:["*"], from:"json", where:"", orderby:[], order: "asc", limit:[] };
		for(i in ops) o[i] = ops[i];

		var result = [];		
		result = this.returnFilter(json,o);
		result = this.returnGroupBy(result,o.groupby,o.aggregat);
		result = this.returnOrderBy(result,o.orderby,o.order);
		result = this.returnLimit(result,o.limit);
				
		return result;
	},
	arrayTrim: function(arr)
	{
		for(i=0; i<arr.length; i++)
		{
			arr[i] = arr[i].trim();
		}
		return arr;
	},
	getAggregatArray: function(arr)
	{
		var aggarr = [];
		for(i=0; i<arr.length; i++)
		{
			result = arr[i].match(/\s*(avg|count|first|last|max|min|sum)\S([a-z0-9]*)\S/);
			if(result != null)
			{
				aggarr[result[2].trim()] = result[1];
			}
		}
		return aggarr;
	},	
	getAggregatFreeArray: function(arr)
	{
		for(i=0; i<arr.length; i++)
		{
			result = arr[i].match(/\s*(avg|count|first|last|max|min|sum)\S([a-z0-9]*)\S/);
			if(result != null)
			{
				arr[i] = result[2].trim();
			}
		}
		return arr;
	},	
	returnFilter: function(json,jsonsql_o){
		
		var jsonsql_scope = eval(jsonsql_o.from);
		var jsonsql_result = [];
		var jsonsql_rc = 0;

		if(jsonsql_o.where == "") 
			jsonsql_o.where = "true";

		for(var jsonsql_i in jsonsql_scope){
			with(jsonsql_scope[jsonsql_i]){
				if(eval(jsonsql_o.where)){
					jsonsql_result[jsonsql_rc++] = this.returnFields(jsonsql_scope[jsonsql_i],jsonsql_o.fields);
				}
			}
		}
		
		return jsonsql_result;
	},
	
	returnFields: function(scope,fields){
		if(fields.length == 0)
			fields = ["*"];
			
		if(fields[0] == "*")
			return scope;
			
		var returnobj = {};
		for(var i in fields)
			returnobj[fields[i]] = scope[fields[i]];
		
		return returnobj;
	},
	
	returnOrderBy: function(result,orderby,order){
		if(orderby.length == 0) 
			return result;
		
		result.sort(function(a,b){

			switch(order.toLowerCase()){
				case "desc": return (eval('a.'+ orderby[0] +' < b.'+ orderby[0]))? 1:-1;
				case "asc":  return (eval('a.'+ orderby[0] +' > b.'+ orderby[0]))? 1:-1;
				//case "descnum": return (eval('a.'+ orderby[0] +' - b.'+ orderby[0]));
				case "descnum":  return (eval(' b.'+ orderby[0] +'.replace(",",".") - a.'+ orderby[0] +'.replace(",",".")'));
				case "ascnum":  return (eval(' a.'+ orderby[0] +'.replace(",",".") - b.'+ orderby[0] +'.replace(",",".")'));
			}
		});

		return result;	
	},	
	returnGroupBy: function(result,groupby,aggregatArr){
		if(groupby.length == 0) 
			return result;
		
		console.log(groupby);
		result = this.groupBy(result, function(item)
		{
			tarr = [];
			for(i=0; i<groupby.length; i++)
			{
				tarr.push(item[groupby[i]]);
			}
			return tarr;
		});
		
		
		var newresult =[];
		for(i=0; i<result.length; i++)
		{
			// First of all copy the first line
			newresult[i] =result[i][0];
			// Do the aggregation
			for (aggregat in aggregatArr)
			{
				//avg|count|first|last|max|min|sum
				if(aggregatArr[aggregat] == "first")
				{
					// First line is already copied
				}
				else if(aggregatArr[aggregat] == "last")
				{
					newresult[i][aggregat] = result[i][result[i].length-1][aggregat];
				}
				else if(aggregatArr[aggregat] == "sum")
				{
					var sum = 0;
					for(aggi=0; aggi<result[i].length; aggi++)
					{
						sum = sum + parseFloat(result[i][aggi][aggregat]);
					}
					newresult[i][aggregat] = sum;
				}
				else if(aggregatArr[aggregat] == "avg")
				{
					var sum = 0;
					for(aggi=0; aggi<result[i].length; aggi++)
					{
						sum = sum + parseFloat(result[i][aggi][aggregat]);
					}
					newresult[i][aggregat] = sum/result[i].length;
				}
				else if(aggregatArr[aggregat] == "max")
				{
					var max = parseFloat(result[i][0][aggregat]);
					for(aggi=0; aggi<result[i].length; aggi++)
					{
						if(max < (newmax=parseFloat(result[i][aggi][aggregat])))
						{
							max = newmax;
						}
						
					}
					newresult[i][aggregat] = max;
				}
				else if(aggregatArr[aggregat] == "min")
				{
					var min = parseFloat(result[i][0][aggregat]);
					for(aggi=0; aggi<result[i].length; aggi++)
					{
						if(min > (newmin=parseFloat(result[i][aggi][aggregat])))
						{
							min = newmin;
						}
						
					}
					newresult[i][aggregat] = min;
				}	
				else if(aggregatArr[aggregat] == "count")
				{
					newresult[i][aggregat] = result[i].length;
				}				
			}
			
		}		
		console.log(newresult);

		return newresult;	
	},
	groupBy: function( array , f ){

	  var groups = {};
	  array.forEach( function( o )
	  {
		 var group = JSON.stringify( f(o) );
		 groups[group] = groups[group] || [];
		 groups[group].push( o );  
	  });
	  return Object.keys(groups).map( function( group )
	  {
		 return groups[group]; 
	  })
	},

	
	returnLimit: function(result,limit){
		switch(limit.length){
			case 0: return result;
			case 1: return result.splice(0,limit[0]);
			//case 2: return result.splice(limit[0]-1,limit[1]);
			case 2: return result.splice(((limit[0]==0) ? 0 : limit[0]-1),limit[1]);
		}
	}
	
};
