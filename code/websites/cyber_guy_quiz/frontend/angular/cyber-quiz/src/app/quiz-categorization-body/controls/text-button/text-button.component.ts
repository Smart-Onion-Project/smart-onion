import { Component, OnInit, Input } from '@angular/core';
import { EventEmitter } from '@angular/common/src/facade/async';

@Component({
  selector: 'app-text-button',
  templateUrl: './text-button.component.html',
  styleUrls: ['./text-button.component.css']
})
export class TextButtonComponent implements OnInit {
  _width : number = 8;
  _usingDefaultWidth = true;
  _topBorderChar : string = "-";
  _bottomBorderChar : string = "-";
  _cornersChar : string = "+";
  _leftBorderChar : string = "|";
  _rightBorderChar : string = "|";
  _text : string = "Submit";

  @Input() name: string;
  @Input('width')
  set width(value: number) {
    this._width = value;
    this._usingDefaultWidth = false;
  }
  @Input('topBorderChar')
  set topBorderChar(value: string) {
    this._topBorderChar = value[0];
  }
  @Input('bottomBorderChar')
  set bottomBorderChar(value: string) {
    this._bottomBorderChar = value[0];
  }
  @Input('cornersChar')
  set cornersChar(value: string) {
    this._cornersChar = value[0];
  }
  @Input('leftBorderChar')
  set leftBorderChar(value: string) {
    this._leftBorderChar = value[0];
  }
  @Input('rightBorderChar')
  set rightBorderChar(value: string) {
    this._rightBorderChar = value[0];
  }
  @Input('text')
  set text(value: string) {
    this._text = value;

    if (this._usingDefaultWidth) {
      this._width = this._text.length + 4;
    }
  }
  private topBorder = "";
  private bottomBorder = "";

  constructor() {}

  ngOnInit() {
  }
}
