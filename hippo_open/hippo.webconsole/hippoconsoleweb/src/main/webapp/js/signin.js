var loginApp = angular.module('loginApp',[])
.controller('loginController',function($scope,$http,$document){
	
	$document.bind("keypress", function(event) {
	    if(event.keyCode == 13){
	    	$scope.login();
	    }
	});
	
	
	
	$scope.login = function(){
		var postData = {};
		if($scope.username == null || $scope.username == ''){
			errorAlert($scope,"用户名不能为空！");
			return false;
		}
		if($scope.password == null || $scope.password == ''){
			errorAlert($scope,"用户密码不能为空！");
			return false;
		}
		var config = {params: {username:$scope.username,passWord:$scope.password}};
		$http.post('logining',postData,config).success(function(data){
			if(data.returnValue == "2"){
				window.location="cluster";
			}else{
				errorAlert($scope,"用户密码错误！");
			}
			
        }).error(function(data) {
        	errorAlert($scope,"登录异常，请联系管理员 ！");
        });
	};
	
	
	
});

var errorAlert = function($scope,msg){
	$scope.msg = msg;
	$('#errorAlert').modal('show').css({
		width:'500px',
		'margin-left':function(){
			return-($(this).width()/2);
		}
	});
};
