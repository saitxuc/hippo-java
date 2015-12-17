$(document).ready(function(){
	
	loadClusterMenuToBase('clusterName');
	
	showZkTree();
	
});


var showZkTree = function (){
	
	$('#zkTree').tree({
	    url:'../server/zk_first_tree_list',
	    method:'get',
	    animate:true,
	    lines:true,
	    onSelect:function(node){
	    	var tableResult = "";
	    	$.post("../server/zk_read",{text:node.id},function(data){
	    		$('#resultSTr').html(data.str);
	    	});
	    	var nPath = node.id;
	    	nPath = nPath.substring(0,nPath.length-6);
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
	    }
	});  
};

var dataReset = function(){
	
	var clusterName = $('#clusterName').combobox('getText');
	var clusterVal = $('#clusterName').combobox('getValue');
	if(clusterVal == null || clusterVal==''){
		jQuery.messager.alert('提示:','请选择需要重置的集群名称');
		return false;
	}
	$.messager.confirm('提示','是否确认重置zk数据，一旦确认数据无法修复，请谨慎操作?',function(r){
		if (r){
			$.post("../server/dataReset",{clusterName:clusterName},function(data){
				if(data.success=="1"){
					jQuery.messager.alert('提示:','zk数据清空成功！');
					$('#zkTree').tree('reload');
				}else{
					jQuery.messager.alert('提示:','zk数据 异常，请去zk手动清空！');
				}
			});
		}else{
			jQuery.messager.alert('提示:','这是您做了一生中最理智的选择！');
		}
	});
};

function loadClusterMenuToBase(controlName){
	$('#'+controlName).combobox({
		editable:false,
		url:"../utils/clusterMenuBase",
		multiple:false,
	    valueField:'id',
	    textField:'text'
	});
}


var treeReload = function (){
	$('#zkTree').tree('reload');
};
