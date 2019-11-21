import { CyberQuizPage } from './app.po';

describe('cyber-quiz App', function() {
  let page: CyberQuizPage;

  beforeEach(() => {
    page = new CyberQuizPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});
