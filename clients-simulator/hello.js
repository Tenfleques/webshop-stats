for(var i = 0; i < 5; ++i)  
    helloWorld();

car = new car("blue");
console.log(car.getColor());

function helloWorld(){
    console.log("hello, World");
}

function car(){
    this.color = arguments[0];
    this.getColor = function(){
        return this.color;
    }
}
