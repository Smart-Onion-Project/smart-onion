import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpModule, Http } from '@angular/http';

import { AppComponent } from './app.component';
import { QuizTitleComponent } from './quiz-title/quiz-title.component';
import { QuizCategorizationBodyComponent } from './quiz-categorization-body/quiz-categorization-body.component';
import { TextButtonComponent } from './quiz-categorization-body/controls/text-button/text-button.component';
import { SlideBarComponent } from './quiz-categorization-body/controls/slide-bar/slide-bar.component';
import { SlideBarGroupComponent } from './quiz-categorization-body/controls/slide-bar-group/slide-bar-group.component';
import { BackendService } from './services/backend.service';

@NgModule({
  declarations: [
    AppComponent,
    QuizTitleComponent,
    QuizCategorizationBodyComponent,
    TextButtonComponent,
    SlideBarComponent,
    SlideBarGroupComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpModule
  ],
  providers: [BackendService],
  bootstrap: [AppComponent]
})
export class AppModule { }
