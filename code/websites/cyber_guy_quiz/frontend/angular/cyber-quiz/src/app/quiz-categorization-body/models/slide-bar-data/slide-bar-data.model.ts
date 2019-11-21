export class SlideBarData {
    public constructor(
        readonly id : string,
        readonly label : string, 
        public value : number, 
        readonly minValue : number, 
        readonly maxValue : number,
        public touched : boolean,
        public timesModifiedByGroup : number) { }
}