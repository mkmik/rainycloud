$(function() {
    console.log("app 3");

    var socket = new io.Socket("localhost",{
        port: 8781
    });
    socket.connect();
    socket.on('connect', function(){ console.log("connected"); }) 
    socket.on('message', function(){  console.log("message");  }) 
    socket.on('disconnect', function(){ console.log("disconnected"); }) 
});