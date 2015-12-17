window.onresize = function() {
    var widths = document.body.scrollWidth;
    var heights = document.documentElement.clientHeight;
    $("#loading-frame").height(heights-115).width(widths);
};
window.onresize();

//上面此段用于保存各个区域的宽高能100%，也保证loading-frame和content的区域一样宽高。

//设置透明度遮罩层，该层大小和IFrame一样，用于遮在IFrame上，并隐藏掉。
$("#loading-frame").css('opacity', .8).hide();

var interval = 0;
//当用户点击左侧导航上的链接的时候开始出现loading效果
var loading_start = function() {
    if (interval) {
        clearInterval(interval);
    }
    $("#loading-frame .bar").css('width', 0);
    $("#loading-frame").show();
    var percent = 0;
    interval = setInterval(function(){
        percent += 10;
        if (percent == 100) {
            percent = 99;
        }
        if (percent < 100) {
            $("#loading-frame .bar").animate({'width': percent + '%'},'slow');
        }
    }, 200);
};

//当iframe已经完全载入后，隐藏loading-frame
var loading_complete = function() {
    $("#loading-frame .bar").animate({'width':'100%'},'fast');
    clearInterval(interval);
    setTimeout(function() {
        $("#loading-frame").fadeOut("slow");
    }, 500);
};
