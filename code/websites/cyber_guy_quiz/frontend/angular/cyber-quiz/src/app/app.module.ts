import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpClientModule } from '@angular/common/http'


import { AppComponent } from './app.component';
import { QuizTitleComponent } from './quiz-title/quiz-title.component';
import { QuizCategorizationBodyComponent } from './quiz-categorization-body/quiz-categorization-body.component';
import { TextButtonComponent } from './quiz-categorization-body/controls/text-button/text-button.component';
import { SlideBarComponent } from './quiz-categorization-body/controls/slide-bar/slide-bar.component';
import { SlideBarGroupComponent } from './quiz-categorization-body/controls/slide-bar-group/slide-bar-group.component';
import { CursorComponent } from './quiz-categorization-body/controls/cursor/cursor.component';

@NgModule({
  declarations: [
    AppComponent,
    QuizTitleComponent,
    QuizCategorizationBodyComponent,
    TextButtonComponent,
    SlideBarComponent,
    SlideBarGroupComponent,
    CursorComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpClientModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
