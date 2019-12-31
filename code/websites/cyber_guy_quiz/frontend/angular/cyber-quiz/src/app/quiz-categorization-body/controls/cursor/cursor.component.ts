import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-cursor',
  templateUrl: './cursor.component.html',
  styleUrls: ['./cursor.component.scss']
})
export class CursorComponent implements OnInit {
  private blinkInterval = null;

  constructor() { }

  ngOnInit() {
    if (this.blinkInterval == null) {
      this.blinkInterval = setInterval(() => {
        var curClassName = document.getElementById('cursor').className;
        if (curClassName == "cursor_visible") {
          curClassName = "cursor_hidden";
        } else {
          curClassName = "cursor_visible";
        }
        document.getElementById('cursor').className = curClassName
      }, 800);
    }
  }
}
