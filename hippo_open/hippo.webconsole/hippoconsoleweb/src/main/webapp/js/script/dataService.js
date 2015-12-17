var app = angular.module('dataServiceApp',[])
.controller('dataServiceController',function($scope,$http){
	
	/** --------------设定页面默认值-------------- */
	$scope.currentPage = 1;
	$scope.pageSize = 9;
    $scope.totalPage = 1;
    $scope.pages = [];
    $scope.endPage = 1;
    $scope.showPage = 5;
    $scope.selected = [];
    
    recordList($scope,$http);
    
    $scope.statusList = [{id:'1',name:'运行中'},{id:'2',name:'已停用'}];
    
    var postData = {};
	var config = {};
	$http.post('../utils/clusterMenuBaseSelected',postData,config).success(function(response){
		$scope.clusterList = response;
	});
    
    
    /** ---------------翻页-------------- */
    $scope.next = function () {
        if ($scope.currentPage < $scope.totalPage) {
            $scope.currentPage++;
            recordList($scope,$http);
        }
    };
 
    $scope.prev = function () {
        if ($scope.currentPage > 1) {
            $scope.currentPage--;
            recordList($scope,$http);
        }
    };
 
    $scope.loadPage = function (page) {
        $scope.currentPage = page;
        recordList($scope,$http);
    };
	
    /**---------------------*/
    
    $scope.search = function(){
		$('#recordSearch').modal('show').css({
    		width:'800px',
    		'margin-left':function(){
    			return-($(this).width()/2);
    		}
    	});
	};
	
	$scope.addWin = function(){
		$('#recordAdd').modal('show').css({
    		width:'800px',
    		'margin-left':function(){
    			return-($(this).width()/2);
    		}
    	});
		
		$scope.add = {
				bucketCount : 0
		};
	};
	
	$scope.toedit = function(id){
		$('#recordEdit').modal('show').css({
    		width:'800px',
    		'margin-left':function(){
    			return-($(this).width()/2);
    		}
    	});
		var postData = {};
		var config = {params:{id:id}};
		$http.post('../server/findServerInfo',postData,config).success(function(response){
			
				if( response != null && response.info !=null)
				{
					var info = response.info;
					
					$scope.edit = {
							clusterName:info.clusterId.toString(),
							url:info.server_id,
							jmxPort:info.jmxPort,
							brokerName:info.brokerName,
							brokerVersion:info.brokerVersion,
							bucketCount:info.bucketCount,
							oldip:info.server_id,
							id:id
					};
				}else{
					errorAlert($scope,"数据异常，请联系管理员！");
				}
				
	    }).error(function(data) {
	    	errorAlert($scope,"数据异常，请联系管理员！");
        });
	};
	
	$scope.doEditSave = function(){
		var msg = "";
    	var cluster = [];
    	if($scope.edit != null ){
    		var clusterId = $scope.edit.clusterName;
    		var url = $scope.edit.url;
    		var jmxPort = $scope.edit.jmxPort;
    		
    		if(clusterId ==null || clusterId == ''){
    			msg = resultMsg(msg,"Cluster Name 不能为空值 ！");
    		}
    		if(url == null || url == ''){
    			msg = resultMsg(msg,"url 不能为空值 ！");
    		}
    		if(jmxPort == null || jmxPort == ''){
    			msg = resultMsg(msg,"jmxPort 不能为空值 ！");
    		}
    		var sel_ = $scope.clusterList;
			var len = sel_.length;
			var index = 0;
			for(var i=0;i<len;i++){
				if(sel_[i].id == clusterId){
					index = i;
				}
			}
			var mName = sel_[index].name;
    		cluster = {
    			clusterId:clusterId,
    			clusterName:mName,
				jmxPort:jmxPort,
				brokerName: $scope.edit.brokerName,
				brokerVersion:$scope.edit.brokerVersion,
				bucketCount:$scope.edit.bucketCount,
				server_id:url,
				old_server_id:$scope.edit.oldip,
				id:$scope.edit.id
    		};
    	}else{
    		msg = "请根据页面要求填写正确的DS信息！";
    	}
    	if(msg != null && msg !=''){
			errorAlert($scope,msg);
		}else{
			confirmAlert($scope,"是否确认修改DS信息？").on(function(e){
				if(e == true){
					var postData = {};
					var config = {params:cluster};
					$http.post('../server/saveServer',postData,config).success(function(response){
						if(response.success == 1){
							successAlert($scope,"服务器修改成功！");
							recordList($scope,$http);
							$('#recordEdit').modal('hide');
						}else if (response.success == 2){
							errorAlert($scope,"服务器ip已存在系统，请确认后再修改！");
						}else{
							errorAlert($scope,"服务器修改失败！");
						}
						
				    }).error(function(data) {
				    	errorAlert($scope,"服务器修改失败！");
			        });
				}
			});
			
		}
    };
	
	$scope.doSave = function(){
		var msg = "";
    	var cluster = [];
    	if($scope.add != null ){
    		var clusterId = $scope.add.clusterName;
    		var url = $scope.add.url;
    		var jmxPort = $scope.add.jmxPort;
    		
    		if(clusterId ==null || clusterId == ''){
    			msg = resultMsg(msg,"Cluster Name 不能为空值 ！");
    		}
    		if(url == null || url == ''){
    			msg = resultMsg(msg,"url 不能为空值 ！");
    		}
    		if(jmxPort == null || jmxPort == ''){
    			msg = resultMsg(msg,"jmxPort 不能为空值 ！");
    		}
    		var sel_ = $scope.clusterList;
			var len = sel_.length;
			var index = 0;
			for(var i=0;i<len;i++){
				if(sel_[i].id == clusterId){
					index = i;
				}
			}
			var mName = sel_[index].name;
    		cluster = {
    			clusterId:clusterId,
    			clusterName:mName,
				jmxPort:jmxPort,
				borkerName: $scope.add.borkerName,
				borkerVersion:$scope.add.borkerVersion,
				bucketCount:$scope.add.bucketCount,
				server_id:url
    		};
    	}else{
    		msg = "请根据页面要求填写正确的DS信息！";
    	}
    	if(msg != null && msg !=''){
			errorAlert($scope,msg);
		}else{
			var postData = {};
			var config = {params:cluster};
			$http.post('../server/addServer',postData,config).success(function(response){
				if(response.success == 1){
					successAlert($scope,"DS信息修改成功！");
					recordList($scope,$http);
					$('#recordAdd').modal('hide');
				}else if (response.success == 2){
					errorAlert($scope,"数据库中已存在该服务器，不能重复创建！！");
				}else{
					errorAlert($scope,"服务器创建失败！");
				}
				
		    }).error(function(data) {
		    	errorAlert($scope,"服务器创建失败！");
	        });
		}
		
		
	};
	
	$scope.view = function(serverId){
		$('#recordView').modal('show').css({
    		width:'1100px',
    		'margin-left':function(){
    			return-($(this).width()/2);
    		}
    	});
		var postData = {};
		var config = {params:{id:serverId}};
		$http.post('../server/findServerInfo',postData,config).success(function(response){
			var dataInfo = response.info;
			$scope.info = {
					id:dataInfo.id,
					clusterName:dataInfo.clusterName,
					server_id:dataInfo.server_id,
					brokerName:dataInfo.brokerName,
					brokerVersion:dataInfo.brokerVersion,
					dataDirectory:dataInfo.dataDirectory,
					started:dataInfo.started,
					memoryLimit:dataInfo.memoryLimit,
					memoryPercentUsage:dataInfo.memoryPercentUsage,
					storeLimit:dataInfo.storeLimit,
					storePercentUsage:dataInfo.storePercentUsage,
					bucketCount:dataInfo.bucketCount,
					status:dataInfo.status,
					masterBucket:dataInfo.masterBucket,
					slaveBucket:dataInfo.slaveBucket
			};
			
			$scope.storeInfo = {
					engineData:dataInfo.engineData,
					currentUsedCapacity:dataInfo.currentUsedCapacity,
					engineName:dataInfo.engineName,
					engineSize:dataInfo.engineSize
			};
			
			$scope.objects = response.info.conn;

			$scope.clientRecords = response.info.client;
			
		});
	};
	
	
	$scope.getconnectionCount = function(server_id,objectName,id){
		var postData = {};
		var config = {params:{server_id:server_id,objectName:objectName,id:id}};
		$http.post('../server/getConnectionCount',postData,config).success(function(response){
			viewAlert($scope,"connectionCount: "+response.connectionCount,"ConnectionCount");
		});
	};
	
	$scope.selectedClient = function(server_id,objectName,id){
		var postData = {};
		var config = {params:{server_id:server_id,objectName:objectName,id:id}};
		$http.post('../server/selectedClient',postData,config).success(function(response){
	
			$scope.clientRecords = response.client;
		});
	};
	
	$scope.getSessionCount = function(server_id,objectName,id){
		var postData = {};
		var config = {params:{server_id:server_id,objectName:objectName,id:id}};
		$http.post('../server/getSessionCount',postData,config).success(function(response){
			viewAlert($scope,"sessionCount: "+response.sessionCount,"SessionCount");
		});
	};
	
	
	$scope.doSearch = function(){
		loading_start();
		recordList($scope,$http);
		$('#recordSearch').modal('hide');
	};
	
	$scope.refrash = function(id){
		loading_start();
		var postData = {};
		var config = {params:{id:id}};
		$http.post('../server/reflashServer',postData,config).success(function(response){
			
			var info = response.info;
			var status = "";
			if(info.status == '1'){
				status = "运行中";
			}else{
				status = "已停用";
			}
			var DataDiv = document.getElementById('div_'+id);
			angular.element(DataDiv).find("table").find("tr").eq(0).find("td").eq(1).html(info.server_id);
			angular.element(DataDiv).find("table").find("tr").eq(1).find("td").eq(1).html(info.brokerName);
			angular.element(DataDiv).find("table").find("tr").eq(2).find("td").eq(1).html(info.brokerVersion);
			angular.element(DataDiv).find("table").find("tr").eq(3).find("td").eq(1).html(info.clusterName);
			angular.element(DataDiv).find("table").find("tr").eq(4).find("td").eq(1).html(status);
			angular.element(DataDiv).find("table").find("tr").eq(5).find("td").eq(1).find("div").eq(0).html(info.bucketCount);
			loading_complete();
		});
	};
	
	$scope.del = function(id){
		confirmAlert($scope,"是否删除dataserver信息，删除之后数据将无法恢复？").on(function(e){
			if(e == true){
				var postData = {};
				var config = {params:{id:id,df:1}};
				$http.post('../server/delServer',postData,config).success(function(data){
					
					if(data.success=="1"){
						recordList($scope,$http);
						successAlert($scope,"数据删除成功！");
				    } else {
				    	errorAlert($scope,"数据异常，请联系管理员！");
				    }
				});
			}
		});
		
	};
    
});

app.filter('nl2br',['$sce',function($sce){
	return function (text){
		return $sce.trustAsHtml(text);
	}
}]);

app.filter('filterStatus',function(){
	var returnStatusName = function(record){
		if(record === 2 || record === '2' ){
			return "已停用";
		}else{
			return "运行中";
		}
	};
	return returnStatusName;
});

var resultMsg = function(msg,content){
	msg = msg + content + "\n";
	return msg;
};

var viewAlert = function($scope,msg,title){
	$scope.msg = msg;
	$scope.title = title;
	$('#viewAlert').modal('show').css({
		width:'500px',
		'margin-left':function(){
			return-($(this).width()/2);
		}
	});
};

var recordList = function($scope,$http){
	loading_start();
	var recordModel = {
			page:$scope.currentPage,
			number:$scope.pageSize,
			start:0,
			status:0
	};
	
	if($scope.search != null){
		var clusterName = $scope.search.clusterName;
		var status = $scope.search.status;
		if(status == null || status == ''){
			status = 0;
		}
		var ip = $scope.search.ip;
		var brokerName = $scope.search.brokerName;
		recordModel = {
				page:$scope.currentPage,
				number:$scope.pageSize,
				start:0,
				status:status,
				clusterName:clusterName,
				brokerName:brokerName,
				ip:ip
		};
	}
	
	var postData = {};
	var config = {params:recordModel};
	$http.post('../server/loadDsList',postData,config).success(function(response){
		$scope.records = response.rows;
		
		var total = response.total;
		$scope.totalPage = Math.ceil(total / $scope.pageSize);
        $scope.endPage = $scope.totalPage;
		
        
        if($scope.totalPage >= $scope.showPage ){//--判断是否小于显示页数
        	if($scope.currentPage == 1){
        		$scope.pages = [
        		                $scope.currentPage,
        		                $scope.currentPage + 1,
        		                $scope.currentPage + 2,
        		                $scope.currentPage + 3,
        		                $scope.currentPage + 4,
        		            ];
        	}else if ($scope.currentPage == 2){
        		$scope.pages = [
        		                $scope.currentPage - 1,
        		                $scope.currentPage ,
        		                $scope.currentPage + 1,
        		                $scope.currentPage + 2,
        		                $scope.currentPage + 3,
        		            ]; 
        	}else if($scope.currentPage > 2 && ($scope.currentPage + 2) <= $scope.totalPage){
        		$scope.pages = [
        		                $scope.currentPage - 2,
        		                $scope.currentPage - 1,
        		                $scope.currentPage ,
        		                $scope.currentPage + 1,
        		                $scope.currentPage + 2,
        		            ];
        	}else{
        		if(($scope.currentPage + 1) == $scope.totalPage){
        			$scope.pages = [
            		                $scope.currentPage - 3,
            		                $scope.currentPage - 2,
            		                $scope.currentPage - 1,
            		                $scope.currentPage ,
            		                $scope.currentPage + 1,
            		            ];
        		}else{
        			$scope.pages = [
            		                $scope.currentPage - 4,
            		                $scope.currentPage - 3,
            		                $scope.currentPage - 2,
            		                $scope.currentPage - 1 ,
            		                $scope.currentPage ,
            		            ];
        		}
        		
        	}
        }else{
        	$scope.pages = [];
        	for(var i=0;i<$scope.totalPage ;i++){
        		var j = i+1;
        		$scope.pages.push(j);
        	}
        }
        loading_complete();
    }).error(function(data) {
    	loading_complete();
    });
};

