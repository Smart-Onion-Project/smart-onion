import { Component, OnInit } from '@angular/core';
import { CategoriesService } from './services/categories.service';
import { BackendService } from '../services/backend.service';

@Component({
  selector: 'app-quiz-categorization-body',
  templateUrl: './quiz-categorization-body.component.html',
  styleUrls: ['./quiz-categorization-body.component.scss'],
  providers: [CategoriesService]
})
export class QuizCategorizationBodyComponent implements OnInit {
  private iterations = 0;
  private cur_question = {
    question_id: "-1",
    metric_id: "-1",
    metric_name: "",
    anomaly_type_id: "-1",
    anomaly_type: "",
    description: "Loading..."
  };

  constructor(private categoriesService : CategoriesService, private backendService : BackendService) { }

  ngOnInit() {
    this.backendService.getNextRandomQuestion().subscribe(question => {
      this.cur_question = question;
    });
    this.categoriesService.refreshCategories();
  }

  onSubmitAnswer(eventData) {
    alert('submit');
  }
  onSkipToGrade(eventData) { 
    alert('skip');
  }
  onResetCategories(eventData) {
    this.categoriesService.forceRefreshCategories();
  }
}
