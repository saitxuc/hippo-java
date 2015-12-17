

var IpColor = function(reslut){
	var ColorArray = new Array("black","pink","orange","green","rosy","blue","red","gray");
	var jsonResult = eval('(' + reslut + ')');
	var colorJson = "";
	if(jsonResult != null){
		var map = jsonResult.tableMap;
		var i = 0;
		var ipStr = "";
		while(map[i] !=null){
			var ipDate = map[i];
			var _len = ipDate.length;
			for(var j=0;j<_len;j++){
				ipStr = ipStr + ipDate[j] +",";
			}
			i++;
		}
		var ipArray = [];
		var newIpArray = [];
		if(ipStr !=null && ipStr.length>0){
			ipStr = ipStr.substring(0,ipStr.length-1);
			ipArray = ipStr.split(",");
			if(ipArray !=null && ipArray.length > 0){
				newIpArray = ipArray.unique1();
			}
		}
		if(newIpArray !=null && newIpArray.length>0){
			var colorIndex = 0;
			for(var j=0;j<newIpArray.length;j++){
				colorJson = colorJson + "'"+newIpArray[j]+"':"+"'"+ColorArray[colorIndex]+"','"+returnCtableIp(newIpArray[j])+"':'"+ColorArray[colorIndex]+"',";
				colorIndex++;
				if(colorIndex >7){
					colorIndex = 0;
				}
			}
		}
		if(colorJson !=null && colorJson.length>0){
			colorJson = "{"+colorJson.substring(0, colorJson.length-1)+"}";
		}
	}
	return colorJson;
};

var reslutTable = function(data,type,IpColorJson){
	
	var reslut = data.str;
	if(reslut !=null && reslut !=''){
		var jsonResult = eval('(' + reslut + ')');
		if(jsonResult != null){
			var map = jsonResult.tableMap;
			var i=0;
			var tr = "";
			var thCout = 0;
			while(map[i] !=null){
				var ipDate = map[i];
				var _len = ipDate.length;
				thCout = _len;
				var td = "";
				if(i==0){
					td = "<td align=\"center\">主机</td>";
				}else{
					td = "<td align=\"center\">备机</td>";
				}
				for(var j=0;j<_len;j++){
					
					if(ipDate[j] == "0" || ipDate[j]==0){
						td = td + "<td align=\"center\"><a href=\"#\" class=\"button white\">"+ipDate[j]+"</a></td>";
					}else{
						var jsonColor = eval('(' + IpColorJson + ')');
						if(jsonColor != null){
							td = td + "<td align=\"center\"><a href=\"#\" class=\"button "+jsonColor[ipDate[j]]+"\">"+ipDate[j]+"</a></td>";
						}else{
							td = td + "<td align=\"center\"><a href=\"#\" class=\"button rosy\">"+ipDate[j]+"</a></td>";
						}
					}
					
				}
				tr = tr +"<tr height=\"30px\">"+td+"</tr>";
				i++;
			}
			var table = "<table width=\"100%\" border=\"0\" cellspacing=\"0\" cellpadding=\"0\" align=\"center\"><tr height=\"30px\">";
			var th= "<td align=\"center\">"+type+"</td>";
			for(var c=0;c<thCout;c++){
				th = th + "<td align=\"center\"><b>"+c+"</b></td>";
			}
			var resultTable = "<div class=\"table-c\" ><center>"+table + th + "</tr>"+tr+"</table></center></div><br>";
			return resultTable;
		}else{
			return "";
		}
	}else{
		return "";
	}
};

Array.prototype.unique1 = function(){
	var res = [this[0]];
	 for(var i = 1; i < this.length; i++){
	  var repeat = false;
	  for(var j = 0; j < res.length; j++){
	   if(this[i] == res[j]){
	    repeat = true;
	    break;
	   }
	  }
	  if(!repeat){
	   res.push(this[i]);
	  }
	 }
	 return res;
};

var returnCtableIp = function(url){
	if(url != null){
		var ipArray = url.split(":");
		if(ipArray !=null && ipArray.length >0){
			if(ipArray.length == 3){
				var newUrl = ipArray[0]+":"+ipArray[2];
				return newUrl;
			}
		}
	}
	return url;
};
