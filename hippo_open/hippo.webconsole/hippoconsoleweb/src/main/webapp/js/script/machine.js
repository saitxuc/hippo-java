var app = angular.module('machineApp',[])
.controller('machineController',function($scope,$http){
	/** --------------设定页面默认值-------------- */
	$scope.currentPage = 1;
	$scope.pageSize = 15;
    $scope.totalPage = 1;
    $scope.pages = [];
    $scope.endPage = 1;
    $scope.showPage = 5;
	
	$scope.statusList = [{id:'1',name:'运行中'},{id:'2',name:'已停用'}];
	recordList($scope,$http);
	
	$scope.search = function(){
		$('#recordSearch').modal('show').css({
    		width:'800px',
    		'margin-left':function(){
    			return-($(this).width()/2);
    		}
    	});
	};
	
	$scope.showView = function(serverId){
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
		recordList($scope,$http);
		$('#recordSearch').modal('hide');
	};
	
	
	
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
});

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


app.filter('nl2br',['$sce',function($sce){
	return function (text){
		return $sce.trustAsHtml(text);
	}
}]);
var recordList = function($scope,$http){
	loading_start();
	var recordModel = {
			status:0,
			page:$scope.currentPage,
			size:$scope.pageSize	
	};
	
	if($scope.search != null ){
		
		var clusterName = $scope.search.clusterName;
		var status = $scope.search.status;
		if(status == null || status == ''){
			status = 0;
		}
		recordModel = {
				clusterName:clusterName,
				status:status,
				page:$scope.currentPage,
				size:$scope.pageSize
		};
	}
	
	var postData = {};
	var config = {params:recordModel};
	$http.post('../server/loadMachineList',postData,config).success(function(response){
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