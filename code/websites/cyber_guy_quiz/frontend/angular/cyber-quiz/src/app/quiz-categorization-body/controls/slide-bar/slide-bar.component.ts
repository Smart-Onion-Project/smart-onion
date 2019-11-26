import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { SlideBarDataUpdate } from '../../../quiz-categorization-body/models/slide-bar-data-update/slide-bar-data-update.model';
import { v4 as uuid } from 'uuid';


@Component({
  selector: 'app-slide-bar',
  templateUrl: './slide-bar.component.html',
  styleUrls: ['./slide-bar.component.scss']
})
export class SlideBarComponent implements OnInit {
  @Output() valueModified : EventEmitter<SlideBarDataUpdate> = new EventEmitter();
  @Output() valueIncreased : EventEmitter<SlideBarDataUpdate> = new EventEmitter();
  @Output() valueDecreased : EventEmitter<SlideBarDataUpdate> = new EventEmitter();
  _id = uuid();
  _labelText = "Label";
  _value = 10;
  _minValue = 0;
  _maxValue = 20;
  _slidebarChar = '-';
  _slidebarValueIndicatorChar = '█';
  _clicked = false;

  constructor() { }

  ngOnInit() {
  }

  @Input('id')
  set id(value : string) {
    this._id = value;
  }

  @Input('label')
  set label(value : string) {
    this._labelText = value;
  }

  @Input('value')
  set value(value : number) {
    this._value = value;
  }

  @Input('minValue')
  set minValue(value : number) {
    this._minValue = value;
  }

  @Input('maxValue')
  set maxValue(value : number) {
    this._maxValue = value;
  }

  @Input('slidebarChar')
  set slidebarChar(value : string) {
    this._slidebarChar = value;
  }

  @Input('slidebarValueIndicatorChar')
  set slidebarValueIndicatorChar(value : string) {
    this._slidebarValueIndicatorChar = value;
  }


  get value() : number {
    return (this._value - this._minValue) / (this._maxValue - this._minValue) * 100
  }
  
  decreaseValue() {
    this._clicked = true;
    if (this._value > (this._minValue + 1)) {
      this.valueModified.emit({id: this._id, label: this._labelText, prev_value: this._value, value: this._value - 1});
      this.valueDecreased.emit({id: this._id, label: this._labelText, prev_value: this._value, value: this._value - 1});
      this._value--;
    }
  }
  increaseValue() {
    this._clicked = true;
    if (this._value < this._maxValue) {
      this.valueModified.emit({id: this._id, label: this._labelText, prev_value: this._value, value: this._value + 1});
      this.valueIncreased.emit({id: this._id, label: this._labelText, prev_value: this._value, value: this._value + 1});
      this._value++;
    }
  }
}
