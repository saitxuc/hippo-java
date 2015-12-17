var app = angular.module('zkBackUpApp',[])
.controller('zkBackUpController',function($scope,$http){
	/** --------------设定页面默认值-------------- */
	$scope.currentPage = 1;
	$scope.pageSize = 15;
    $scope.totalPage = 1;
    $scope.pages = [];
    $scope.endPage = 1;
    $scope.showPage = 5;


	recordList($scope,$http);
	
	$scope.search = function(){
		$('#recordSearch').modal('show').css({
    		width:'800px',
    		'margin-left':function(){
    			return-($(this).width()/2);
    		}
    	});
	};

	
	$scope.doSearch = function(){
		recordList($scope,$http);
		$('#recordSearch').modal('hide');
	};
	
	$scope.showConfig = function (config){
		viewAlert($scope,config,"Config View");
		
	};
	
	$scope.showTable = function (id){
		$('#recordTable').modal('show').css({
    		width:'1100px',
    		'margin-left':function(){
    			return-($(this).width()/2);
    		}
    	});
		
		var postData = {};
		var config = {params:{id:id}};
		$http.post('../backup/showTables',postData,config).success(function(response){
			$scope.recordTables = response.list;
		});
		
	};
	
	$scope.showServers = function(id){
		var postData = {};
		var config = {params:{id:id}};
		$http.post('../backup/showDataServers',postData,config).success(function(response){
			$scope.recordDataServers = response.list;
			
		});
		
		$('#recordServers').modal('show').css({
    		width:'1100px',
    		'margin-left':function(){
    			return-($(this).width()/2);
    		}
    	});
		
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


app.filter('configFilter',function(){
	var returnConfig = function(record){
		var len = record.length;
		if(len > 50){
			return record.substring(0,50) + " ...";
		}else{
			return record;
		}
	};
	return returnConfig;
});

var viewAlert = function($scope,msg,title){
	$scope.msg = msg;
	$scope.title = title;
	$('#viewAlert').modal('show').css({
		width:'800px',
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
			page:$scope.currentPage,
			size:$scope.pageSize	
	};
	
	if($scope.search != null ){
		
		var clusterName = $scope.search.clusterName;
		var version = $scope.search.version;
		if(version == null || version == ''){
			version = 0;
		}
		recordModel = {
				version:version,
				clusterName:clusterName,
				page:$scope.currentPage,
				size:$scope.pageSize
		};
	}
	
	var postData = {};
	var config = {params:recordModel};
	$http.post('../backup/loadBackupList',postData,config).success(function(response){
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