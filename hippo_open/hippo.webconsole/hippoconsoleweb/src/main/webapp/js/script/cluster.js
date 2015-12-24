var app = angular.module('clusterApp',[])
.controller('clusterController',function($scope,$http){
	/** --------------设定页面默认值-------------- */
	$scope.currentPage = 1;
	$scope.pageSize = 10;
    $scope.totalPage = 1;
    $scope.pages = [];
    $scope.endPage = 1;
    $scope.showPage = 5;
    $scope.selected = [];
    $scope.oldClusterName = "";
    
    $scope.server = {
    		currentPage : 1,
    		pageSize:5,
    		totalPage:1,
    		pages:[],
    		endPage:1,
    		showPage:5,
    		clusterName:""
    };
    
    /** -----------------执行页面数据操作---------------  */
    recordList($scope,$http);
    $scope.typeList = [{id:'mdb',name:'mdb'},{id:'levelDb',name:'levelDb'}];
    $scope.statusList = [{id:'1',name:'未生效'},{id:'2',name:'已生效'}];
    $scope.funList = [{id:'1',name:'GET'},{id:'2',name:'REMOVE'},{id:'3',name:'INC'},{id:'4',name:'DECR'},{id:'5',name:'SET'},{id:'6',name:'SSET'},{id:'7',name:'HSET'},{id:'8',name:'LSET'}];
    
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
        recordServerList($scope,$http);
    };
    
    $scope.server.next = function () {
        if ($scope.server.currentPage < $scope.server.totalPage) {
            $scope.server.currentPage++;
            recordServerList($scope,$http);
        }
    };
 
    $scope.server.prev = function () {
        if ($scope.server.currentPage > 1) {
            $scope.server.currentPage--;
            recordServerList($scope,$http);
        }
    };
 
    $scope.server.loadPage = function (page) {
        $scope.server.currentPage = page;
        recordServerList($scope,$http);
    };
    
   /** ------------显示对话框------------ */ 
    $scope.search = function(){
    	showSearchDiv();
    };
    
    $scope.cmdWin = function(clusterName){
    	$('#recordCmd').modal('show').css({
    		width:'800px',
    		'margin-left':function(){
    			return-($(this).width()/2);
    		}
    	});
    	$scope.cmd={
    			clusterName : clusterName
    	};
    	allShow();
    };
    
    /**
     * 显示ds
     */
    $scope.tableWin = function(clusterName){
    	$('#recordTable').modal('show').css({
    		width:'1200px',
    		'margin-left':function(){
    			return-($(this).width()/2);
    		}
    	});
    	
    	var tableResult = "";
    	var nPath = "hippo/cluster/"+clusterName+"/tables/";
    	var mtable = nPath + "mtable";
    	$.post("../server/zk_read",{text:mtable},function(data){
    		var IpColorStr = IpColor(data.str);
    		tableResult = tableResult + reslutTable(data,"mtable",IpColorStr);
        	var dtable = nPath + "dtable";
        	$.post("../server/zk_read",{text:dtable},function(data){
        		tableResult = tableResult + reslutTable(data,"dtable",IpColorStr);
        		var ctable = nPath + "ctable";
    	    	$.post("../server/zk_read",{text:ctable},function(data){
    	    		tableResult = tableResult + reslutTable(data,"ctable",IpColorStr);
    	    		$('#resultTable').empty();
    	    		$('#resultTable').append(tableResult);
    	    	});
        	});
    	});
    };
    
    $scope.addWin = function(){
    	$('#recordAdd').modal('show').css({
    		width:'750px',
    		'margin-left':function(){
    			return-($(this).width()/2);
    		}
    	});
    };
    
    $scope.editWin = function(){
    	
    	if($scope.selected == null || $scope.selected.length == 0){
    		errorAlert($scope,"请选中需要修改的集群！");
    		return false;
    	}else if($scope.selected.length > 1){
    		errorAlert($scope,"一次只能修改一条信息！");
    		return false;
    	}else{
    		
    		var postData = {};
    		var config = {params:{id:$scope.selected[0]}};
    		$http.post('../cluster/viewCluster',postData,config).success(function(response){
    			var data = response.clusterView;
    			$('#recordEdit').modal('show').css({
    	    		width:'750px',
    	    		'margin-left':function(){
    	    			return-($(this).width()/2);
    	    		}
    	    	});
    			
    			$scope.edit = {
						clusterName:data.clusterName,
						dbType:data.dbType,
						copyCount:data.copyCount,
						bucketLimit:data.bucketLimit,
						replicatePort:data.replicatePort,
						hashCount:data.hashCount,
						content:data.details
				};
    			$scope.oldClusterName = data.clusterName;
    			$scope.selected = [];
    		}).error(function(data) {
    			$scope.selected = [];
	        });
    		
    	}
    };
    
    $scope.getDataServer = function(clusterName){
    	$scope.server.clusterName = clusterName;
    	recordServerList($scope,$http);
    	$('#recordServerWin').modal('show').css({
    		width:'1200px',
    		'margin-left':function(){
    			return-($(this).width()/2);
    		}
    	});
    };
    
   /**  ------------实际操作方法------------ */
    $scope.doSearch = function(){
    	recordList($scope,$http);
    	$('#recordSearch').modal('hide');
    };
    
    $scope.doSave = function(){
    	var msg = "";
    	var cluster = [];
    	if($scope.add != null ){
    		var clusterName = $scope.add.clusterName;
    		var dbType = $scope.add.dbType;
    		var copyCount = $scope.add.copyCount;
    		var bucketLimit = $scope.add.bucketLimit;
    		var replicatePort = $scope.add.replicatePort;
    		var hashCount = $scope.add.hashCount;
    		
    		if(clusterName ==null || clusterName == ''){
    			msg = resultMsg(msg,"Cluster Name 不能为空值 ！");
    		}
    		if(dbType == null || dbType == ''){
    			msg = resultMsg(msg,"DB Type 不能为空值 ！");
    		}
    		if(copyCount == null || copyCount == ''){
    			msg = resultMsg(msg,"Copy Count 不能为空值 ！");
    		}
    		if(bucketLimit == null || bucketLimit == ''){
    			msg = resultMsg(msg,"Bucket Limit 不能为空值 ！");
    		}
    		if(replicatePort == null || replicatePort == ''){
    			msg = resultMsg(msg,"Replicate Port 不能为空值 ！");
    		}
    		if(hashCount == null || hashCount == ''){
    			msg = resultMsg(msg,"Hash Count 不能为空值 ！");
    		}
    		cluster = {
				clusterName:clusterName,
				dbType:dbType,
				copyCount:copyCount,
				bucketLimit:bucketLimit,
				replicatePort:replicatePort,
				hashCount:hashCount,
				details:$scope.add.content
    		};
    	}else{
    		msg = "请根据页面要求填写正确的集群信息！";
    	}
		if(msg != null && msg !=''){
			errorAlert($scope,msg);
		}else{
			var postData = {};
			var config = {params:cluster};
			$http.post('../cluster/addCluster',postData,config).success(function(response){
				if(response.success == 1){
					successAlert($scope,"新集群信息创建成功！");
					$('#recordAdd').modal('hide');
					recordList($scope,$http);
					$scope.add = {
							clusterName:"",
							dbType:"",
							copyCount:"",
							bucketLimit:"",
							replicatePort:"",
							hashCount:"",
							details:""
					};
					
				}else if (response.success == 2){
					errorAlert($scope,"集群"+cluster.clusterName+"已存在系统，不能重复创建集群！");
				}else{
					errorAlert($scope,"集群创建失败，请联系管理员！");
				}
				
		    }).error(function(data) {
	        });
		}
    };
	
    
    $scope.dodel = function(){
    	if($scope.selected == null || $scope.selected.length == 0){
    		errorAlert($scope,"请选中需要删除的集群！");
    		return false;
    	}else if($scope.selected.length > 1){
    		errorAlert($scope,"一次只能删除一条信息！");
    		return false;
    	}else{
    		confirmAlert($scope,"是否删除集群信息，删除之后数据将无法恢复？").on(function(e){
    			if(e == true){
    				var postData = {};
    				var config = {params: {id:$scope.selected[0]}};
    				$http.post('../cluster/delCluster',postData,config).success(function(response){
    					var success = response.success;
    					if(success == 1){
    						successAlert($scope,"集群数据删除成功！");
    						recordList($scope,$http);
    					}else{
    						errorAlert($scope,"集群数据删除失败！");
    					}
    					$scope.selected = [];
    			    }).error(function(data) {
    			    	errorAlert($scope,"集群数据删除失败！");
    			    	$scope.selected = [];
    		        });
    			}
    		});
    	};
    	
    };
    
    $scope.doSend = function(){
    	if($scope.selected == null || $scope.selected.length == 0){
    		errorAlert($scope,"请选中需要推送的集群！");
    		return false;
    	}else if($scope.selected.length > 1){
    		errorAlert($scope,"一次只能推送一条信息！");
    		return false;
    	}else{
    		confirmAlert($scope,"是否推送集群信息，推送之后，数据将无法删除？").on(function(e){
    			if(e == true){
    				var postData = {};
    				var config = {params: {id:$scope.selected[0],status:2}};
    				$http.post('../cluster/sendCluster',postData,config).success(function(response){
    					var success = response.success;
    					if(success == 1){
    						successAlert($scope,"集群数据推送成功！");
    						recordList($scope,$http);
    					}else{
    						errorAlert($scope,"集群数据推送失败！");
    					}
    					$scope.selected = [];
    			    }).error(function(data) {
    			    	errorAlert($scope,"集群数据推送失败！");
    			    	$scope.selected = [];
    		        });
    			}
    		});
    	};
    };
    
    $scope.doEditSave = function(){
    	var msg = "";
    	var cluster = [];
    	if($scope.edit != null ){
    		var clusterName = $scope.edit.clusterName;
    		var dbType = $scope.edit.dbType;
    		var copyCount = $scope.edit.copyCount;
    		var bucketLimit = $scope.edit.bucketLimit;
    		var replicatePort = $scope.edit.replicatePort;
    		var hashCount = $scope.edit.hashCount;
    		var id = $scope.selected[0];
    		if(clusterName ==null || clusterName == ''){
    			msg = resultMsg(msg,"Cluster Name 不能为空值 ！");
    		}
    		if(dbType == null || dbType == ''){
    			msg = resultMsg(msg,"DB Type 不能为空值 ！");
    		}
    		if(copyCount == null || copyCount == ''){
    			msg = resultMsg(msg,"Copy Count 不能为空值 ！");
    		}
    		if(bucketLimit == null || bucketLimit == ''){
    			msg = resultMsg(msg,"Bucket Limit 不能为空值 ！");
    		}
    		if(replicatePort == null || replicatePort == ''){
    			msg = resultMsg(msg,"Replicate Port 不能为空值 ！");
    		}
    		if(hashCount == null || hashCount == ''){
    			msg = resultMsg(msg,"Hash Count 不能为空值 ！");
    		}
    		cluster = {
    			id:id,
				clusterName:clusterName,
				dbType:dbType,
				copyCount:copyCount,
				bucketLimit:bucketLimit,
				replicatePort:replicatePort,
				hashCount:hashCount,
				details:$scope.edit.content,
				oldClusterName:$scope.oldClusterName
    		};
    	}else{
    		msg = "请根据页面要求填写正确的集群信息！";
    	}
    	if(msg != null && msg !=''){
			errorAlert($scope,msg);
		}else{
			confirmAlert($scope,"是否确认修改集群信息，修改之后需要重新推送，并且服务器端重启后才能生效？").on(function(e){
				if(e == true){
					var postData = {};
					var config = {params:cluster};
					$http.post('../cluster/editCluster',postData,config).success(function(response){
						if(response.success == 1){
							successAlert($scope,"集群信息修改成功！");
							recordList($scope,$http);
							$('#recordEdit').modal('hide');
						}else if (response.success == 2){
							errorAlert($scope,"集群"+cluster.clusterName+"已存在系统，不能重复创建集群！");
						}else{
							errorAlert($scope,"集群修改失败，请联系管理员！");
						}
						$scope.selected = [];
				    }).error(function(data) {
				    	errorAlert($scope,"集群修改失败，请联系管理员！");
				    	$scope.selected = [];
			        });
				}
			});
			
		}
    };
    
    $scope.funChange = function(){
    	var selectId = $scope.cmd.funName;
    	if(selectId === '1' || selectId === 1){
    		allHide();
    	}else if (selectId === '2' || selectId === 2){
			allHide();
		}else if (selectId === '3' || selectId === 3){
			allShow();
		}else if (selectId === '4' || selectId === 4){
			allShow();
		}else if (selectId === '5' || selectId === 5){
			lastHide();
		}else if (selectId === '6' || selectId === 6){
			lastHide();
		}else if (selectId === '7' || selectId === 7){
			lastHide();
		}else if (selectId === '8' || selectId === 8){
			lastHide();
		}else { //--selectId === '9'
			allShow();
		}
    };
    
    $scope.cmdSubmit = function(){
    	var selectId = $scope.cmd.funName;
    	if(selectId == null || selectId == '' || selectId == 0){
    		errorAlert($scope,"请选择方法名！");
    		return false;
    	}
    	
    	var clusterName = $scope.cmd.clusterName;
    	if(clusterName ==null || clusterName == ''){
    		errorAlert($scope,"集群名称不能为空！");
    		return false;
    	}
    	
    	var key = $scope.cmd.key;
    	if(key == null || key ==''){
    		errorAlert($scope,"键不能为空！");
    		return false;
    	}
    	
    	if(selectId != null && selectId !=''){
    		if (selectId == 3 || selectId == 4 || selectId == 9){
    			var val = $scope.cmd.value;
    			var defaultVal =  $scope.cmd.defaultVal;
    			var expireTime = $scope.cmd.expireTime;
    			if(val == null || val ==''){errorAlert($scope,"值不能为空！"); return false;}
    			if(defaultVal == null || defaultVal ==''){errorAlert($scope,"默认值不能为空！"); return false;}
    			if(expireTime == null || expireTime ==''){errorAlert($scope,"过期时间不能为空！"); return false;}
    			
    		}else if (selectId == 5 || selectId == 6 || selectId == 7 || selectId == 8){
    			var val = $scope.cmd.value;
    			var expireTime = $scope.cmd.expireTime;
    			if(val == null || val ==''){errorAlert($scope,"值不能为空！"); return false;}
    			if(expireTime == null || expireTime ==''){errorAlert($scope,"过期时间不能为空！"); return false;}
    		}
    	}
    	var clusterCmd = {
    			clusterName:clusterName,
    			key:key,
    			defaultVal:$scope.cmd.defaultVal,
    			expireTime:$scope.cmd.expireTime,
    			val:$scope.cmd.value,
    			cmdType:selectId
    	};
    	var postData = {};
		var config = {params:clusterCmd};
    	$http.post('../cluster/cmdCluster',postData,config).success(function(data){
    		var isSuccess = data.success;
    		var result = "";
    		if(isSuccess == "1"){
    			result = result + " status : 操作成功 ！！\n" ;
    			if(data.dataType != null && data.dataType !=''){
    				result = result + " data type : "+data.dataType +" \n";
    			}
    			if(data.dataContent != null && data.dataContent !=''){
    				result = result + " data content : "+ data.dataContent + " \n";
    			}
    			if(data.dataVersion !=null && data.dataVersion != ''){
    				result = result + " data version : "+ data.dataVersion ;
    			}
    			
    		}else if(isSuccess == '2'){
    			result = result + " status : 操作失败 ！！\n" ;
    			if(data.responseCode != null && data.responseCode != ''){
    				result = result + " response code : "+data.responseCode +" \n";
    			}
    			if(data.errorLog != null && data.errorLog !=''){
    				result = result + " error log : "+ data.errorLog;
    			}
    			
    		}else if(isSuccess == '3'){
    			result = result + " status : 操作失败 ！！\n" ;
    			if(data.errorLog != null && data.errorLog != ''){
    				result = "error log : "+ data.errorLog;
    			}
    			
    		}else{
    			errorAlert($scope,"只有admin用户才能操作！");
    			return false;
    		}
    		$scope.cmd.result = result;
	    }).error(function(data) {
	    	errorAlert($scope,"数据异常，请联系管理员！");
        });
    };

    
    
    /** --------------checkbox------------ */
    $scope.updateSelection = function($event, id){
		var checkbox = $event.target;
		var action = (checkbox.checked?'add':'remove');
		updateSelected(action,id,checkbox.name);
	};

	$scope.isSelected = function(id){
		return $scope.selected.indexOf(id)>=0;
	};
	
	var updateSelected = function(action,id,name){
		if(action == 'add' && $scope.selected.indexOf(id) == -1){
			$scope.selected.push(id);
			//$scope.selectedTags.push(name);
		}
		if(action == 'remove' && $scope.selected.indexOf(id)!=-1){
			var idx = $scope.selected.indexOf(id);
			$scope.selected.splice(idx,1);
			//$scope.selectedTags.splice(idx,1);
		}
	};
    
	
});

var resultMsg = function(msg,content){
	msg = msg + content + "<br />";
	return msg;
};

app.filter('nl2br',['$sce',function($sce){
	return function (text){
		return $sce.trustAsHtml(text);
	}
}]);

app.filter('filterStatus',function(){
	var returnStatusName = function(record){
		if(record == 2 || record == '2' ){
			return "已生效";
		}else{
			return "未生效";
		}
	};
	return returnStatusName;
});

app.filter('filterServerStatus',function(){
	var returnStatusName = function(record){
		if(record == 2 || record == '2' ){
			return "已停用";
		}else{
			return "运行中";
		}
	};
	return returnStatusName;
});

var showSearchDiv = function(){
	$('#recordSearch').modal('show').css({
		width:'750px',
		'margin-left':function(){
			return-($(this).width()/2);
		}
	});
};

var errorAlert = function($scope,msg){
	$scope.msg = msg;
	$('#errorAlert').modal('show').css({
		width:'500px',
		'margin-left':function(){
			return-($(this).width()/2);
		}
	});
};

var successAlert = function($scope,msg){
	$scope.msg = msg;
	$('#successAlert').modal('show').css({
		width:'500px',
		'margin-left':function(){
			return-($(this).width()/2);
		}
	});
};

var confirmAlert = function($scope,msg){
	$scope.msg = msg;
	$('#confirm').modal('show').css({
		width:'500px',
		'margin-left':function(){
			return-($(this).width()/2);
		}
	});
	return {
        on: function (callback) {
          if (callback && callback instanceof Function) {
            $scope.doOK = function(){callback(true)};
            $scope.doCancel = function(){callback(false)};
          }
        }
    };
};


var recordList = function($scope,$http){
	loading_start();
	var recordModel = {
			page:$scope.currentPage,
			size:$scope.pageSize
	};
	
	if($scope.search != null ){
		
		var clusterName = $scope.search.clusterName;
		var dbType = $scope.search.dbType;
		var replicatePort = $scope.search.replicatePort;
		var status = $scope.search.status;
		
		if(replicatePort == null || replicatePort == ''){
			replicatePort = 0;
		}
		recordModel = {
				clusterName:clusterName,
				dbType:dbType,
				replicatePort:replicatePort,
				status:status,
				page:$scope.currentPage,
				size:$scope.pageSize
		};
	}
	
	var postData = {};
	var config = {params:recordModel};
	$http.post('../cluster/loadClusterList',postData,config).success(function(response){
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

var recordServerList = function($scope,$http){
	
	var recordModel = {
			page:$scope.server.currentPage,
			size:$scope.server.pageSize,
			clusterName:$scope.server.clusterName
	};
	
	var postData = {};
	var config = {params:recordModel};
	$http.post('../server/loadServerList',postData,config).success(function(response){
		$scope.serverRecords = response.rows;
		
		var total = response.total;
		$scope.server.totalPage = Math.ceil(total / $scope.server.pageSize);
        $scope.server.endPage = $scope.server.totalPage;
		
        
        if($scope.server.totalPage >= $scope.server.showPage ){//--判断是否小于显示页数
        	if($scope.server.currentPage == 1){
        		$scope.serverPages = [
        		                $scope.server.currentPage,
        		                $scope.server.currentPage + 1,
        		                $scope.server.currentPage + 2,
        		                $scope.server.currentPage + 3,
        		                $scope.server.currentPage + 4,
        		            ];
        	}else if ($scope.server.currentPage == 2){
        		$scope.serverPages = [
        		                $scope.server.currentPage - 1,
        		                $scope.server.currentPage ,
        		                $scope.server.currentPage + 1,
        		                $scope.server.currentPage + 2,
        		                $scope.server.currentPage + 3,
        		            ]; 
        	}else if($scope.server.currentPage > 2 && ($scope.server.currentPage + 2) <= $scope.server.totalPage){
        		$scope.serverPages = [
        		                $scope.server.currentPage - 2,
        		                $scope.server.currentPage - 1,
        		                $scope.server.currentPage ,
        		                $scope.server.currentPage + 1,
        		                $scope.server.currentPage + 2,
        		            ];
        	}else{
        		if(($scope.server.currentPage + 1) == $scope.server.totalPage){
        			$scope.serverPages = [
            		                $scope.server.currentPage - 3,
            		                $scope.server.currentPage - 2,
            		                $scope.server.currentPage - 1,
            		                $scope.server.currentPage ,
            		                $scope.server.currentPage + 1,
            		            ];
        		}else{
        			$scope.serverPages = [
            		                $scope.server.currentPage - 4,
            		                $scope.server.currentPage - 3,
            		                $scope.server.currentPage - 2,
            		                $scope.server.currentPage - 1 ,
            		                $scope.server.currentPage ,
            		            ];
        		}
        		
        	}
        }else{
        	$scope.serverPages = [];
        	for(var i=0;i<$scope.server.totalPage ;i++){
        		var j = i+1;
        		$scope.serverPages.push(j);
        	}
        }
    });
};




var allShow = function(){
	$('#val').show();
	$('#val_').show();
	$('#defaultVal').show();
	$('#defaultVal_').show();
	$('#expireTime').show();
	$('#expireTime_').show();
	$('#miao_').show();
};

var allHide = function(){
	$('#val').hide();
	$('#val_').hide();
	$('#defaultVal').hide();
	$('#defaultVal_').hide();
	$('#expireTime').hide();
	$('#expireTime_').hide();
	$('#miao_').hide();
};

var lastHide = function(){
	$('#val').show();
	$('#val_').show();
	$('#defaultVal').hide();
	$('#defaultVal_').hide();
	$('#expireTime').show();
	$('#expireTime_').show();
	$('#miao_').show();
};