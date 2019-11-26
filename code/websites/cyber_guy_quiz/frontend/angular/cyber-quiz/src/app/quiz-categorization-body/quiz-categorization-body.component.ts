import { Component, OnInit } from '@angular/core';
import { CategoriesService } from './services/categories.service';

@Component({
  selector: 'app-quiz-categorization-body',
  templateUrl: './quiz-categorization-body.component.html',
  styleUrls: ['./quiz-categorization-body.component.scss'],
  providers: [CategoriesService]
})
export class QuizCategorizationBodyComponent implements OnInit {
  private iterations = 0;
  constructor(private categoriesService : CategoriesService) { }

  ngOnInit() {
    this.categoriesService.refreshCategories();
  }

  onSubmitAnswer(eventData) {
    alert('submit');
  }
  onSkipToGrade(eventData) { 
    alert('skip');
  }
}
